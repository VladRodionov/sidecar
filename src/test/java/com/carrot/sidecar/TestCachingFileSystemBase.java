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

import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.Files.createTempDirectory;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrot.cache.Cache;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.Utils;
import com.carrot.sidecar.util.CacheType;
import com.carrot.sidecar.util.SidecarConfig;

/**
 * This test creates instance of a caching file system 
 *
 */
public abstract class TestCachingFileSystemBase {
  
  private static final Logger LOG = LoggerFactory.getLogger(TestCachingFileSystemBase.class);

  protected FileSystem fs;
  
  protected static URI extDirectory;
  
  protected URI cacheDirectory;
  
  protected URI writeCacheDirectory;  
  
  protected CacheType cacheType = CacheType.OFFHEAP;
  
  /**
   * Subclasses can override
   */

  protected long fileSize = 10L * 1024 * 1024 * 1024;
    
  protected long fileCacheSize = 5L * 1024 * 1024 * 1024;
  
  protected int fileCacheSegmentSize = 64 * 1024 * 1024;
  
  protected long offheapCacheSize = 1L * 1024 * 1024 * 1024;
  
  protected int offheapCacheSegmentSize = 4 * 1024 * 1024;
  
  protected long metaCacheSize = 1L << 30;
  
  protected int metaCacheSementSize = 4 * (1 << 20);
  
  protected double zipfAlpha = 0.9;
  
  protected long writeCacheMaxSize = 5L * (1 << 30);
    
  protected int pageSize = 1 << 12; // 1MB
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
  
  @BeforeClass
  public static void setupClass() throws IOException {
    if (Utils.getJavaVersion() < 11) {
      LOG.warn("Java 11+ is required to run test");
      System.exit(-1);
    }
    extDirectory = createTempDirectory("ext").toUri();
    
  }
  
  @AfterClass
  public static void tearDown() throws IOException {
    TestUtils.deletePathRecursively(extDirectory.getPath());
    LOG.info("Deleted {}", extDirectory.getPath());
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
    SidecarCachingFileSystem.dispose();
    checkState(cacheDirectory != null);
    TestUtils.deletePathRecursively(cacheDirectory.getPath());
    LOG.info("Deleted {}", cacheDirectory);
    checkState(writeCacheDirectory != null);
    TestUtils.deletePathRecursively(writeCacheDirectory.getPath());
    LOG.info("Deleted {}", writeCacheDirectory);
  }
  
  protected FileSystem cachingFileSystem() throws IOException {
    Configuration conf = getConfiguration();
    conf.set("fs.file.impl", SidecarTestFileSystem.class.getName());
    conf.set("fs.file.impl.disable.cache", Boolean.TRUE.toString());
    conf.set(SidecarConfig.SIDECAR_WRITE_CACHE_MODE_KEY, WriteCacheMode.ASYNC.getMode());
    conf.set(SidecarConfig.SIDECAR_WRITE_CACHE_SIZE_KEY, Long.toString(writeCacheMaxSize));
    conf.set(SidecarConfig.SIDECAR_WRITE_CACHE_URI_KEY, writeCacheDirectory.toString());
    conf.set(SidecarConfig.SIDECAR_TEST_MODE_KEY, Boolean.TRUE.toString());
    // Set global cache directory
    conf.set(CarrotConfig.CACHE_ROOT_DIR_PATH_KEY, cacheDirectory.getPath());
    CarrotConfig carrotCacheConfig = CarrotConfig.getInstance();
    // Set meta cache 
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.META_CACHE_NAME, metaCacheSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.META_CACHE_NAME, metaCacheSementSize);
    
    FileSystem fs = FileSystem.get(extDirectory, conf);
    // Set meta caching enabled
    SidecarCachingFileSystem cfs = ((SidecarTestFileSystem) fs).getCachingFileSystem();
    cfs.setMetaCacheEnabled(true);
    return fs;
  }
  
  protected abstract Configuration getConfiguration() ;
  
  @Test
  public void testCRUD() throws IOException {
    LOG.info("Test Create - Update - Rename - Delete");
    Path p = new Path(new Path(extDirectory.toString()), "testfile");
    FSDataOutputStream os = fs.create(p);
    
    byte[] buf = new byte[1 << 16];
    Random r = new Random();
    r.nextBytes(buf);
    
    os.write(buf, 0, buf.length);
    // It starts thread on close!!!
    os.close();
    
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    FileStatus fst =  fs.getFileStatus(p);
    assertEquals(buf.length, fst.getLen());
    
    SidecarCachingFileSystem sidecar = ((CachingFileSystem) fs).getCachingFileSystem();
    FileSystem writeCacheFS = sidecar.getWriteCacheFS();
    Path cachedPath = sidecar.remoteToCachingPath(p);
    fst = writeCacheFS.getFileStatus(cachedPath);
    assertEquals(buf.length, fst.getLen());
    
    Path newPath = new Path(new Path(extDirectory.toString()), "testfile1");
    boolean b = fs.rename(p, newPath);
    
    assertTrue(b);
    
    // Verify old files do not exist
    Path newCachedPath = sidecar.remoteToCachingPath(newPath);

    assertFalse(fs.exists(p));
    assertFalse(writeCacheFS.exists(cachedPath));
    assertTrue(fs.exists(newPath));
    assertTrue(writeCacheFS.exists(newCachedPath));
    
    // Read
    
    FSDataInputStream is = fs.open(newPath);
    is.readFully(buf);
    is.close();
    // Now we have some data in the cache
    Cache dataCache = SidecarCachingFileSystem.getDataCache();
    Cache metaCache = SidecarCachingFileSystem.getMetaCache();
    
    assertTrue(dataCache.getStorageUsed() > buf.length);
    assertEquals(1, metaCache.activeSize());
    
    boolean result = fs.delete(newPath, true);
    assertTrue(result);
    
    assertFalse(fs.exists(newPath));
    assertFalse(writeCacheFS.exists(newCachedPath));
    
    fs.close();

    
  }

   
  
}
