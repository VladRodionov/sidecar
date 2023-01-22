/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.carrot.sidecar;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.Files.createTempDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.Callable;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrot.cache.Cache;
import com.carrot.cache.controllers.AQBasedAdmissionController;
import com.carrot.cache.controllers.MinAliveRecyclingSelector;
import com.carrot.cache.eviction.SLRUEvictionPolicy;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.Epoch;
import com.carrot.cache.util.Utils;
import com.carrot.sidecar.util.SidecarConfig;


/**
 * TODO: test with write cache populated
 */

public class TestSidecarCachingInputStream {
  
  protected static boolean fillFile = false;
  
  protected static File sourceFile;
  
  protected static boolean skipTest = false;
  
  protected static long fileSize = 100L * 1024 * 1024 * 1024;

  private static final Logger LOG = LoggerFactory.getLogger(TestSidecarCachingInputStream.class);

  private URI cacheDirectory;
    
  private long cacheSize = 20L * 1024 * 1024 * 1024;
  
  private int cacheSegmentSize = 64 * 1024 * 1024;
  
  private double zipfAlpha = 0.9;
  
  private Cache cache;
  
  int pageSize = 1 << 12; // 1MB
  // If access is random buffer size must be small
  // reads must be aligned to a page size
  int ioBufferSize = 2 * pageSize;//2 << 20; // 2MB
  
  String domainName;
  
    
  @BeforeClass
  public static void setupClass() throws IOException {
    int javaVersion = Utils.getJavaVersion();
    if (javaVersion < 11) {
      skipTest = true;
      LOG.warn("Skipping {} java version 11 and above is required", TestSidecarCachingInputStream.class.getName());
      return;
    }
    sourceFile = TestUtils.createTempFile();
    if (fillFile) {
      TestUtils.fillRandom(sourceFile, fileSize);
    }
  }
  
  @AfterClass
  public static void tearDown() {
      if (skipTest) {
        return;
      }
      sourceFile.delete();
      LOG.info("Deleted {}", sourceFile.getAbsolutePath());
  }
  
  @Before
  public void setup()
          throws IOException
  {
    if (skipTest) return;
    LOG.info("{} BeforeMethod", Thread.currentThread().getName());  
    this.cacheDirectory = createTempDirectory("carrot_cache").toUri();
    Epoch.reset();
  }
  
  @After
  public void close() throws IOException {
    if (skipTest) return;
    unregisterJMXMetricsSink(cache);
    cache.dispose();
    checkState(cacheDirectory != null);
    TestUtils.deletePathRecursively(cacheDirectory.getPath());
    LOG.info("Deleted {}", cacheDirectory);
  }
  
  private Cache createCache(boolean acEnabled) throws IOException {
    SidecarConfig cacheConfig = SidecarConfig.getInstance();
    cacheConfig
      .setDataPageSize(pageSize)
      .setIOBufferSize(ioBufferSize)
      .setJMXMetricsEnabled(true);
    
    CarrotConfig carrotCacheConfig = CarrotConfig.getInstance();
    
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.DATA_CACHE_NAME, cacheSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.DATA_CACHE_NAME, cacheSegmentSize);
    carrotCacheConfig.setCacheEvictionPolicy(SidecarConfig.DATA_CACHE_NAME, SLRUEvictionPolicy.class.getName());
    carrotCacheConfig.setRecyclingSelector(SidecarConfig.DATA_CACHE_NAME, MinAliveRecyclingSelector.class.getName());
    if (acEnabled) {
      carrotCacheConfig.setAdmissionController(SidecarConfig.DATA_CACHE_NAME, AQBasedAdmissionController.class.getName());
    }
    Configuration configuration = TestUtils.getHdfsConfiguration(cacheConfig, carrotCacheConfig);
    cache = TestUtils.createDataCacheFromHdfsConfiguration(configuration);
    LOG.info("Recycling selector={}", cache.getEngine().getRecyclingSelector().getClass().getName());
    
    boolean metricsEnabled = cacheConfig.isJMXMetricsEnabled();
    if (metricsEnabled) {
      domainName = cacheConfig.getJMXMetricsDomainName();
      this.cache.registerJMXMetricsSink(domainName);
    }
    return cache;
  }
  
  private void unregisterJMXMetricsSink(Cache cache) {
    if (domainName == null) {
      return;
    }
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
    ObjectName name;
    try {
      name = new ObjectName(String.format("%s:type=cache,name=%s",domainName, cache.getName()));
      mbs.unregisterMBean(name); 
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("unregisterMBean",e);
    }
    Cache victimCache = cache.getVictimCache();
    if (victimCache != null) {
      unregisterJMXMetricsSink(victimCache);
    }
  }
  
  @Test
  public void testSidecarCachingInputStreamACDisabled () throws Exception {
    if (skipTest) return;

    LOG.info("Java version={}", Utils.getJavaVersion());
    this.cache = createCache(false);
    runTestRandomAccess();
  }
  
  @Test
  public void testSidecarCachingInputStreamACEnabledSeq () throws Exception {
    if (skipTest) return;

    this.cache = createCache(true);
    runTestRandomSequentialAccess();
  }
  
  @Test
  public void testCarrotCachingInputStreamACEnabled () throws IOException {
    if (skipTest) return;

    LOG.info("Java version={}", Utils.getJavaVersion());
    this.cache = createCache(true);
    Runnable r = () -> {
      try {
        runTestRandomAccess();
      } catch (Exception e) {
        LOG.error("testCarrotCachingInputStreamACEnabled",e);
        fail();
        System.exit(-1);
      }
    };
    
    Thread[] runners = new Thread[4];
    for(int i = 0; i < runners.length; i++) {
      runners[i] = new Thread(r);
      runners[i].start();
    }
    for(int i = 0; i < runners.length; i++) {
      try {
        runners[i].join();
      } catch (InterruptedException e) {
      }
    }
  }

  @Test
  public void testCarrotCachingInputStreamACDisabledSeq () throws Exception {
    if (skipTest) return;

    this.cache = createCache(false);
    runTestRandomSequentialAccess();
  }
  
  @Test
  public void testCarrotCachingInputStreamNotPositionalReads() throws Exception {
    if (skipTest) return;

    this.cache = createCache(false);
    runTestSequentialAccess();
  }
  
  @Test
  public void testCarrotCachingInputStreamHeapByteBuffer() throws Exception {
    if (skipTest) return;

    this.cache = createCache(false);
    runTestSequentialAccessByteBuffer(false);
  }
  
  @Test
  public void testCarrotCachingInputStreamDirectByteBuffer() throws Exception {
    if (skipTest) return;

    this.cache = createCache(false);
    runTestSequentialAccessByteBuffer(true);
  }
  
  protected Callable<FSDataInputStream> getExternalStream() throws IOException {
    
    return () -> new FSDataInputStream(new VirtualFileInputStream(fileSize));
  }
  
  protected Callable<FSDataInputStream> getWriteCacheStream() throws IOException {
    
    return () -> null;
  }
  
  private void runTestRandomAccess() throws Exception {
    
    Callable<FSDataInputStream> extStreamCall = getExternalStream();
    Callable<FSDataInputStream> cacheStreamCall = getWriteCacheStream();
    
    long fileLength = fileSize;
    try (
        SidecarCachingInputStream carrotStream = new SidecarCachingInputStream(cache,
            new Path(sourceFile.toURI()), extStreamCall, cacheStreamCall, fileLength, pageSize, ioBufferSize);) 
    {
      FSDataInputStream cacheStream = new FSDataInputStream(carrotStream);
      FSDataInputStream extStream = extStreamCall.call();
      
      int numRecords = (int) (fileSize / pageSize);
      ZipfDistribution dist = new ZipfDistribution(numRecords, this.zipfAlpha);

      int numIterations = numRecords;

      Random r = new Random();
      byte[] buffer = new byte[pageSize];
      byte[] controlBuffer = new byte[pageSize];
      long startTime = System.currentTimeMillis();
      long totalRead = 0;
      
      for (int i = 0; i < numIterations; i++) {
        long startLoop = System.nanoTime();
        long sampleStart = System.nanoTime();
        long n = dist.sample();
        long sampleEnd = System.nanoTime();
        long offset = (n - 1) * pageSize;

        int requestOffset = r.nextInt(pageSize / 2);
        // This can cross
        int requestSize = r.nextInt(pageSize/* - requestOffset */);
        offset += requestOffset;
        requestSize = (int) Math.min(requestSize, fileLength - offset);

        long t1 = System.nanoTime();
        extStream.readFully(offset, controlBuffer, 0, requestSize);
        long t2 = System.nanoTime();
        cacheStream.readFully(offset, buffer, 0, requestSize);
        long t3 = System.nanoTime();
        //
        totalRead += requestSize;
        boolean result = Utils.compareTo(buffer, 0, requestSize, controlBuffer, 0, requestSize) == 0;
        long endLoop = System.nanoTime();
        if (!result) {
          LOG.error("i={} file length={} offset={} requestSize={}", i, fileSize, offset, requestSize);
        }
        assertTrue(result);
        if (i > 0 && i % 100000 == 0) {
          LOG.info("{}: read {} offset={} size={} direct read={} cache read={} sample={} loop={}", 
            Thread.currentThread().getName(), i, offset, requestSize,
            (t2 - t1) / 1000, (t3 - t2) / 1000, (sampleEnd - sampleStart)/1000, (endLoop - startLoop)/ 1000);
        }
      }
      long endTime = System.currentTimeMillis();
      LOG.info("Test finished in {}ms total read={}", (endTime - startTime), totalRead);      
      TestUtils.printStats(cache);
    }
  }
  
  private void runTestRandomSequentialAccess() throws Exception {
    Callable<FSDataInputStream> extStreamCall = getExternalStream();
    Callable<FSDataInputStream> cacheStreamCall = getWriteCacheStream();
    long fileLength = fileSize;

    try (SidecarCachingInputStream carrotStream = new SidecarCachingInputStream(cache,
        new Path(sourceFile.toURI()), extStreamCall, cacheStreamCall, fileLength, pageSize, ioBufferSize);) {

      FSDataInputStream cacheStream = new FSDataInputStream(carrotStream);
      FSDataInputStream extStream = extStreamCall.call();
      
      int numRecords = (int) (fileSize / pageSize);
      ZipfDistribution dist = new ZipfDistribution(numRecords, this.zipfAlpha);

      int numIterations = numRecords;

      Random r = new Random();
      byte[] buffer = new byte[pageSize];
      byte[] controlBuffer = new byte[pageSize];
      long startTime = System.currentTimeMillis();
      long totalRead = 0;
      for (int i = 0; i < numIterations; i++) {
        int accessSize = 8 * 1024;
        long n = dist.sample();
        long offset = (n - 1) * pageSize;
        int requestOffset = r.nextInt(pageSize);
        int requestSize = this.ioBufferSize;
        offset += requestOffset;
        requestSize = (int) Math.min(requestSize, fileLength - offset);
        long t1 = System.nanoTime();
        int m = requestSize / accessSize;
        if (m == 0) {
          accessSize = requestSize;
        }
        for (int k = 0; k < m; k++) {

          extStream.readFully(offset, controlBuffer, 0, accessSize);
          cacheStream.readFully(offset, buffer, 0, accessSize);
          assertTrue(Utils.compareTo(buffer, 0, accessSize, controlBuffer, 0, accessSize) == 0);
          offset += accessSize;
          totalRead += accessSize;
        }
        long t2 = System.nanoTime();

        if (i > 0 && i % 10000 == 0) {
          LOG.info("read {} offset={} size={}  read time={}", i, offset, requestSize,
            (t2 - t1) / 1000);
        }
      }
      long endTime = System.currentTimeMillis();
      LOG.info("Test finished in {}ms total read={}", (endTime - startTime), totalRead);
      TestUtils.printStats(cache);
    }
  }
  
  private void runTestSequentialAccess() throws Exception {
    Callable<FSDataInputStream> extStreamCall = getExternalStream();
    Callable<FSDataInputStream> cacheStreamCall = getWriteCacheStream();
    long fileLength = fileSize;

    try (SidecarCachingInputStream carrotStream = new SidecarCachingInputStream(cache,
        new Path(sourceFile.toURI()), extStreamCall, cacheStreamCall, fileLength, pageSize, ioBufferSize);) {

      FSDataInputStream cacheStream = new FSDataInputStream(carrotStream);
      FSDataInputStream extStream = extStreamCall.call();
      int requestSize = Math.min(pageSize, 8 * 1024);
      int numRecords = 1000;
      byte[] buffer = new byte[pageSize];
      byte[] controlBuffer = new byte[pageSize];
      long startTime = System.currentTimeMillis();
      long totalRead = 0;
      for (int i = 0; i < numRecords; i++) {

        int readExt =  extStream.read(controlBuffer, 0, requestSize);
        int readCache = cacheStream.read(buffer, 0, requestSize);
        //LOG.info("ext pos=%d cache pos=%d\n", extStream.getPos(), cacheStream.getPos());
        assertEquals(readExt, requestSize);
        assertEquals(readCache, requestSize);
        assertTrue(Utils.compareTo(buffer, 0, requestSize, controlBuffer, 0, requestSize) == 0);
        assertEquals(extStream.getPos(), cacheStream.getPos());
        if ( i > 0 && i % 100000 == 0) {
          LOG.info("read {}", i);
        }
      }
      long endTime = System.currentTimeMillis();
      LOG.info("Test finished in {}ms total read requests={}", (endTime - startTime), totalRead);
      TestUtils.printStats(cache);
    }
  }
  
  private void runTestSequentialAccessByteBuffer(boolean direct) throws Exception {
    Callable<FSDataInputStream> extStreamCall = getExternalStream();
    Callable<FSDataInputStream> cacheStreamCall = getWriteCacheStream();
    long fileLength = fileSize;

    try (SidecarCachingInputStream carrotStream = new SidecarCachingInputStream(cache,
        new Path(sourceFile.toURI()), extStreamCall, cacheStreamCall, fileLength, pageSize, ioBufferSize);) {

      FSDataInputStream cacheStream = new FSDataInputStream(carrotStream);
      FSDataInputStream extStream = extStreamCall.call();
      int requestSize = 8 * 1024;
      int numRecords = 1000;
      ByteBuffer buffer = direct? ByteBuffer.allocateDirect(requestSize): ByteBuffer.allocate(requestSize);
      byte[] controlBuffer = new byte[requestSize];
      long startTime = System.currentTimeMillis();
      for (int i = 0; i < numRecords; i++) {

        int readExt =  extStream.read(controlBuffer);
        int readCache = cacheStream.read(buffer);
        assertEquals(readExt, requestSize);
        assertEquals(readCache, requestSize);
        assertEquals(extStream.getPos(), cacheStream.getPos());
        buffer.flip();
        assertTrue(Utils.compareTo(buffer, requestSize, controlBuffer, 0, requestSize) == 0);
        if ( i > 0 && i % 100000 == 0) {
          LOG.info("read {}", i);
        }
        buffer.clear();
      }
      long endTime = System.currentTimeMillis();
      LOG.info("Test finished in {}ms total read requests={}", (endTime - startTime), numRecords);
      TestUtils.printStats(cache);
    }
  }
}
