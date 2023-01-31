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
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
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

import com.carrot.cache.Cache;
import com.carrot.cache.controllers.AQBasedAdmissionController;
import com.carrot.cache.controllers.MinAliveRecyclingSelector;
import com.carrot.cache.eviction.SLRUEvictionPolicy;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.Epoch;
import com.carrot.cache.util.Utils;
import com.carrot.sidecar.util.SidecarConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSidecarCachingInputStreamStress {
    
  private static final Logger LOG = LoggerFactory.getLogger(TestSidecarCachingInputStreamBase.class);

  private URI cacheDirectory;
  
  private File sourceFile;
  
  private long cacheSize = 500L * (1 << 30);
  
  private long cacheSegmentSize = 128L * (1 << 20);
  
  private long fileSize = 2000L * (1 << 20);

  private double zipfAlpha = 0.9;
  
  private Cache cache;
  
  int pageSize;
  
  int ioBufferSize;
  
  String domainName;
    
  @BeforeClass
  public void setupClass() throws IOException {
    this.sourceFile = TestUtils.createTempFile();
  }
  
  @AfterClass
  public void tearDown() {

      sourceFile.delete();
      LOG.info("Deleted {}", sourceFile.getAbsolutePath());
  }
  
  @Before
  public void setup()
          throws IOException
  {
    LOG.info("{} BeforeMethod", Thread.currentThread().getName());  
    this.cacheDirectory = createTempDirectory("carrot_cache").toUri();
      Epoch.reset();
  }
  
  @After
  public void close() throws IOException {
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
    
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.DATA_CACHE_FILE_NAME, cacheSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.DATA_CACHE_FILE_NAME, cacheSegmentSize);
    carrotCacheConfig.setCacheEvictionPolicy(SidecarConfig.DATA_CACHE_FILE_NAME, SLRUEvictionPolicy.class.getName());
    carrotCacheConfig.setRecyclingSelector(SidecarConfig.DATA_CACHE_FILE_NAME, MinAliveRecyclingSelector.class.getName());
    if (acEnabled) {
      carrotCacheConfig.setAdmissionController(SidecarConfig.DATA_CACHE_FILE_NAME, AQBasedAdmissionController.class.getName());
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
      LOG.error("unregisterJMX", e);
    }
    Cache victimCache = cache.getVictimCache();
    if (victimCache != null) {
      unregisterJMXMetricsSink(victimCache);
    }
  }

  @Test
  public void testCarrotCachingInputStreamACEnabled () throws Exception {
    LOG.info("Java version={}", Utils.getJavaVersion());
    this.cache = createCache(true);
    Runnable r = () -> {
      try {
        runTestRandomAccess();
      } catch (Exception e) {
        LOG.error("testCarrotCachingInputStreamACEnabled", e);
        fail();
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
            new Path(this.sourceFile.toURI()), extStreamCall, cacheStreamCall, fileLength, pageSize, ioBufferSize);) 
    {
      FSDataInputStream cacheStream = new FSDataInputStream(carrotStream);
      FSDataInputStream extStream = extStreamCall.call();
      
      int numRecords = (int) (fileSize / pageSize);
      ZipfDistribution dist = new ZipfDistribution(numRecords, this.zipfAlpha);

      int numIterations = numRecords * 1000;

      Random r = new Random();
      byte[] buffer = new byte[pageSize];
      byte[] controlBuffer = new byte[pageSize];
      long startTime = System.currentTimeMillis();
      long totalRead = 0;
      
      for (int i = 0; i < numIterations; i++) {
        long n = dist.sample();
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
        if (!result) {
          LOG.error("i={} file length={} offset={} requestSize={}", i, fileSize, offset, requestSize);
        }
        assertTrue(result);
        if (i > 0 && i % 10000 == 0) {
          LOG.info("{}: read {} offset={} size={} direct read={} cache read={}", 
            Thread.currentThread().getName(), i, offset, requestSize,
            (t2 - t1) / 1000, (t3 - t2) / 1000);
        }
      }
      long endTime = System.currentTimeMillis();
      LOG.info("Test finished in {}ms total read={}", (endTime - startTime), totalRead);
      TestUtils.printStats(cache);
    }
  }
}
