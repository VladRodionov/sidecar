/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import static java.nio.file.Files.createTempDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrot.cache.Cache;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.Utils;
import com.carrot.sidecar.fs.file.SidecarLocalFileSystem;

/**
 * This test creates instance of a caching file system 
 *
 */
public abstract class TestCachingFileSystemMultithreadedBase {
  
  protected static final Logger LOG = LoggerFactory.getLogger(TestCachingFileSystemMultithreadedBase.class);
  protected FileSystem fs;
  protected URI cacheDirectory;
  protected URI writeCacheDirectory;    
  protected SidecarDataCacheType cacheType = SidecarDataCacheType.OFFHEAP;
  
  /**
   * Subclasses can override
   */
   
  protected long fileCacheSize = 5L * 1024 * 1024 * 1024;
  protected int fileCacheSegmentSize = 64 * 1024 * 1024;
  protected long offheapCacheSize = 1L * 1024 * 1024 * 1024;
  protected int offheapCacheSegmentSize = 4 * 1024 * 1024;
  protected long metaCacheSize = 1L << 30;
  protected int metaCacheSegmentSize = 4 * (1 << 20);
  protected double zipfAlpha = 0.9;
  protected long writeCacheMaxSize = 5L * (1 << 30);
  protected long externalDataSize = 10L << 30; // 10 GB
  protected int pageSize = 1 << 16; // 64KB (For HBase)
  // If access is random buffer size must be small
  // reads must be aligned to a page size
  protected int ioBufferSize = 2 * pageSize;//2 << 20; // 2MB
  protected int scavThreads = 1;
  protected boolean aqEnabledFile = false;
  protected boolean aqEnabledOffheap = false;
  protected double aqStartRatio = 0.5;
  
  // Hybrid cache
  protected boolean hybridCacheInverseMode = true;
  protected boolean victim_promoteOnHit = true;
  protected double victim_promoteThreshold = 0.99;

  protected transient int numExtFiles;  
  protected transient long extDataSize;
  protected transient long counter;
  protected long fileSize = 100 * 1 << 20; // 100MB
  protected int numThreads = 4;
  
  protected long testDuration = 600000; // 10 min
  
  @BeforeClass
  public static void setupClass() throws IOException {
    if (Utils.getJavaVersion() < 11) {
      LOG.warn("Java 11+ is required to run test");
      System.exit(-1);
    }    
  }
  
  @Before
  public void setUp() throws IOException {
    this.cacheDirectory = createTempDirectory("carrot_cache").toUri();
    this.writeCacheDirectory = createTempDirectory("write_cache").toUri();
    try {
      this.fs = cachingFileSystem();
    } catch (Exception e) {
      LOG.error("setUp", e);
      fail();
    } 
  }
  
  @After 
  public void close() throws IOException {
    TestUtils.deletePathRecursively(cacheDirectory.getPath());
    LOG.info("Deleted {}", cacheDirectory);
    TestUtils.deletePathRecursively(writeCacheDirectory.getPath());
    LOG.info("Deleted {}", writeCacheDirectory);
    Path workingDirectory = fs.getWorkingDirectory();
    boolean result = fs.delete(workingDirectory, true);
    if (result) {
      LOG.info("Deleted {}", workingDirectory);
    } else {
      LOG.info("Failed to delete {}", workingDirectory);
    }
    SidecarCachingFileSystem.dispose();
  }
  
  protected FileSystem cachingFileSystem() throws IOException, URISyntaxException {
    Configuration conf = getConfiguration();
    conf.set("fs.file.impl", SidecarLocalFileSystem.class.getName());
    conf.set("fs.file.impl.disable.cache", Boolean.TRUE.toString());
    conf.set(SidecarConfig.SIDECAR_WRITE_CACHE_MODE_KEY, WriteCacheMode.ASYNC_CLOSE.getMode());
    conf.set(SidecarConfig.SIDECAR_WRITE_CACHE_SIZE_KEY, Long.toString(writeCacheMaxSize));
    conf.set(SidecarConfig.SIDECAR_WRITE_CACHE_URI_KEY, writeCacheDirectory.toString());
    conf.setBoolean(SidecarConfig.SIDECAR_TEST_MODE_KEY, true);
    conf.setBoolean(SidecarConfig.SIDECAR_JMX_METRICS_ENABLED_KEY, false);
    // Set global cache directory
    conf.set(CarrotConfig.CACHE_ROOT_DIR_PATH_KEY, cacheDirectory.getPath());
    // Files are immutable after creation
    conf.setBoolean(SidecarConfig.SIDECAR_REMOTE_FILES_MUTABLE_KEY, false);
    
    CarrotConfig carrotCacheConfig = CarrotConfig.getInstance();
    // Set meta cache 
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.META_CACHE_NAME, metaCacheSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.META_CACHE_NAME, metaCacheSegmentSize);
    
    FileSystem fs = FileSystem.get(new URI ("file:///"), conf);
    // Set meta caching enabled
    SidecarCachingFileSystem cfs = ((SidecarLocalFileSystem) fs).getCachingFileSystem();
    cfs.setMetaCacheEnabled(true);
    return fs;
  }
  
  protected abstract Configuration getConfiguration() ;
  
 
  @Test
  public void testStress() throws IOException {
    LOG.info("Continuously creates new files, delete old ones and read data");
    final  Path workingDir = fs.getWorkingDirectory();
    Runnable writer = () -> {

      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < testDuration) {
        try {
          Path p = new Path(workingDir, "testfile" + counter);
          FSDataOutputStream os = fs.create(p, true);
          writeFileData(os, fileSize);
          extDataSize += fileSize;
          numExtFiles++;
          if (extDataSize > externalDataSize) {
            long startIndex = counter - numExtFiles;
            while (true) {
              Path pp = new Path(workingDir, "testfile" + startIndex);
              if (!fs.exists(pp)) {
                startIndex++;
                continue;
              }
              boolean res = fs.delete(pp, false);
              assertTrue(res);
              numExtFiles--;
              extDataSize -= fileSize;
              LOG.info("Deleted {}", pp);
              break;
            }
          }
          LOG.info("Created {}", p);
          counter++;
          Thread.sleep(2000);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };

    Runnable reader = () -> {
      int numRecords = (int) (externalDataSize / fileSize);
      ZipfDistribution dist = new ZipfDistribution(numRecords, this.zipfAlpha);
      int k = 0;
      Random r = new Random(Thread.currentThread().getId());

      byte[] buf = new byte[pageSize];
      byte[] bbuf = new byte[pageSize];
      int count = 0;
      long start = System.currentTimeMillis();

      while (System.currentTimeMillis() - start < testDuration) {
        int n = dist.sample();
        if (counter - n < 0) {
          n = (int) ((double) counter * fileSize / externalDataSize * n);
        }
        Path p = new Path(workingDir, "testfile" + (counter - n));
        try {
          if (!fs.exists(p)) {
            Thread.sleep(100);
            continue;
          }
          FileStatus status = fs.getFileStatus(p);
          
          long fileSize = status.getLen();

          if (fileSize < this.fileSize) {
            Thread.sleep(100);
            continue;
          }
          FSDataInputStream is = fs.open(p);
          for (k = 0; k < 1000; k++) {
            long pos = Math.abs(r.nextLong()) % fileSize;
            int toRead = (int) Math.min(pageSize, fileSize - pos);
            is.readFully(pos, bbuf, 0, toRead);
            generateData(pos, buf, 0, toRead);
            assertEquals(0, Utils.compareTo(buf, 0, toRead, bbuf, 0, toRead));
            count++;
          }
          is.close();
          if (count % 10000 == 0) {
            LOG.info("Thread {} count={} RPS={}", Thread.currentThread().getName(), count,
              count * 1000L / (System.currentTimeMillis() - start));
          }
        } catch (FileNotFoundException e) {
          LOG.error("FNFE {}", e.getMessage());
        } catch (IOException e) {
          LOG.error("Reader ", e);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    // Start writer
    Thread writerThread = new Thread(writer);
    writerThread.start();
    Thread[] readers = new Thread[numThreads];

    for (int i = 0; i < numThreads; i++) {
      readers[i] = new Thread(reader);
      readers[i].start();
    }

    try {
      writerThread.join();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    for (Thread t : readers) {
      try {
        t.join();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    printStats();
  }

  private void printStats() {
    Cache dataCache = SidecarCachingFileSystem.getDataCache();
    Cache metaCache = SidecarCachingFileSystem.getMetaCache();
    dataCache.printStats();
    metaCache.printStats();
  }

  private void writeFileData(FSDataOutputStream stream, long fileSize) throws IOException {
    byte[] buf = new byte[1000000];
    long pos = 0;
    while (pos < fileSize) {
      int limit = (int) Math.min(buf.length, fileSize - pos);
      generateData(pos, buf, 0, limit);
      stream.write(buf, 0, limit);
      pos += limit;
    }
    stream.close();
  }
  
  private void generateData(long position, byte[] buf, int bufOff, int bufLen) {
    for (int i = bufOff; i < bufOff + bufLen; i++) {
      buf[i] = (byte)((position + i) % 256);
    }
  }
}
