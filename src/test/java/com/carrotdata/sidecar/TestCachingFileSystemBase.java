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
package com.carrotdata.sidecar;

import static com.carrotdata.sidecar.util.Utils.checkState;
import static java.nio.file.Files.createTempDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.Utils;
import com.carrotdata.sidecar.fs.file.SidecarLocalFileSystem;
import com.carrotdata.sidecar.util.Statistics;

/**
 * This test creates instance of a caching file system 
 *
 */
public abstract class TestCachingFileSystemBase {
  
  protected static final Logger LOG = LoggerFactory.getLogger(TestCachingFileSystemBase.class);

  protected FileSystem fs;
  
  protected static URI extDirectory;
  
  protected URI cacheDirectory;
  
  protected URI writeCacheDirectory;  
  
  protected SidecarDataCacheType cacheType = SidecarDataCacheType.OFFHEAP;
  
  /**
   * Subclasses can override
   */

  protected long fileSize = 10L * 1024 * 1024 * 1024;
    
  protected long fileCacheSize = 5L * 1024 * 1024 * 1024;
  
  protected int fileCacheSegmentSize = 64 * 1024 * 1024;
  
  protected long offheapCacheSize = 1L * 1024 * 1024 * 1024;
  
  protected int offheapCacheSegmentSize = 4 * 1024 * 1024;
  
  protected long metaCacheSize = 1L << 30;
  
  protected int metaCacheSegmentSize = 4 * (1 << 20);
  
  protected double zipfAlpha = 0.9;
  
  protected long writeCacheMaxSize = 5L * (1 << 30);
    
  protected int pageSize = 1 << 16; // 64KB (For HBase)
  // If access is random buffer size must be small
  // reads must be aligned to a page size
  protected int ioBufferSize = 4 * pageSize;//2 << 20; // 2MB
      
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
    checkState(cacheDirectory != null, "data cache directory is not specified");
    TestUtils.deletePathRecursively(cacheDirectory.getPath());
    LOG.info("Deleted {}", cacheDirectory);
    checkState(writeCacheDirectory != null, "write cache directory is not specified");
    TestUtils.deletePathRecursively(writeCacheDirectory.getPath());
    LOG.info("Deleted {}", writeCacheDirectory);
  }
  
  protected FileSystem cachingFileSystem() throws IOException {
    Configuration conf = getConfiguration();
    conf.set("fs.file.impl", SidecarLocalFileSystem.class.getName());
    conf.setBoolean("fs.file.impl.disable.cache", true);
    conf.set(SidecarConfig.SIDECAR_WRITE_CACHE_MODE_KEY, WriteCacheMode.ASYNC_CLOSE.getMode());
    conf.set(SidecarConfig.SIDECAR_WRITE_CACHE_SIZE_KEY, Long.toString(writeCacheMaxSize));
    conf.set(SidecarConfig.SIDECAR_WRITE_CACHE_URI_KEY, writeCacheDirectory.toString());
    conf.setBoolean(SidecarConfig.SIDECAR_TEST_MODE_KEY, true);
    conf.setBoolean(SidecarConfig.SIDECAR_JMX_METRICS_ENABLED_KEY, false);
    // Set global cache directory
    conf.set(CacheConfig.CACHE_DATA_DIR_PATHS_KEY, cacheDirectory.getPath());
    // Files are immutable after creation
    conf.setBoolean(SidecarConfig.SIDECAR_REMOTE_FILES_MUTABLE_KEY, false);
    
    CacheConfig carrotCacheConfig = CacheConfig.getInstance();
    // Set meta cache 
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.META_CACHE_NAME, metaCacheSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.META_CACHE_NAME, metaCacheSegmentSize);
    
    FileSystem fs = FileSystem.get(extDirectory, conf);
    // Set meta caching enabled
    SidecarCachingFileSystem cfs = ((SidecarLocalFileSystem) fs).getCachingFileSystem();
    cfs.setMetaCacheEnabled(true);
    return fs;
  }
  
  protected abstract Configuration getConfiguration() ;
  
  @Test
  public void testCRUD() throws IOException {
    LOG.info("Test Create - Update - Rename - Delete file");
    Path workingDir = fs.getWorkingDirectory();
    Path p = new Path(workingDir, "testfile");
    FSDataOutputStream os = fs.create(p);
    
    byte[] buf = new byte[1 << 18];// 256K, 4 data pages
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
    // First call must return CachedFileStatus
    assertTrue(fst instanceof CachedFileStatus);
    assertEquals(buf.length, fst.getLen());
    
    SidecarCachingFileSystem sidecar = ((RemoteFileSystemAccess) fs).getCachingFileSystem();
    FileSystem writeCacheFS = sidecar.getWriteCacheFS();
    Path cachedPath = sidecar.remoteToCachingPath(p);
    fst = writeCacheFS.getFileStatus(cachedPath);
    assertEquals(buf.length, fst.getLen());
    
    Statistics stats = sidecar.getStatistics();
    stats.reset();
    // Now read data first time
    FSDataInputStream is = fs.open(p);
    is.readFully(buf);
    is.close();
        
    long fromCache = stats.getTotalBytesReadDataCache();
    long fromRemoteFS = stats.getTotalBytesReadRemote();
    long fromWriteCache = stats.getTotalBytesReadWriteCache();
    // ALL reads must come from write cache
    assertEquals(0, (int) fromRemoteFS);
    assertEquals(0, (int) fromCache);
    assertEquals(buf.length, (int) fromWriteCache);
    // All reads must be from data cache now
    
    stats.reset();
    
    is = fs.open(p);
    is.readFully(buf);
    is.close();

    fromCache = stats.getTotalBytesReadDataCache();
    fromRemoteFS = stats.getTotalBytesReadRemote();
    fromWriteCache = stats.getTotalBytesReadWriteCache();
    // ALL reads must come from write cache
    assertEquals(0, (int) fromRemoteFS);
    assertEquals(buf.length,(int) fromCache);
    assertEquals(0, (int) fromWriteCache);
    
    // Now we have some data in the cache
    Cache dataCache = SidecarCachingFileSystem.getDataCache();
    Cache metaCache = SidecarCachingFileSystem.getMetaCache();
    
    Path newPath = new Path(workingDir, "testfile1");
    boolean b = fs.rename(p, newPath);
    
    assertTrue(b);
    
    // Verify old files do not exist
    Path newCachedPath = sidecar.remoteToCachingPath(newPath);

    assertFalse(fs.exists(p));
    assertFalse(writeCacheFS.exists(cachedPath));
    assertTrue(fs.exists(newPath));
    assertTrue(writeCacheFS.exists(newCachedPath));
    
    // Read renamed file
    stats.reset();
    
    is = fs.open(newPath);
    is.readFully(buf);
    is.close();
    // Now we MUST read again everyting from write cache
    
    fromCache = stats.getTotalBytesReadDataCache();
    fromRemoteFS = stats.getTotalBytesReadRemote();
    fromWriteCache = stats.getTotalBytesReadWriteCache();
    
    // ALL reads must come from write cache
    assertEquals(0, (int) fromRemoteFS);
    assertEquals(0,(int) fromCache);
    assertEquals(buf.length, (int) fromWriteCache);
    
    assertTrue(dataCache.getStorageUsedActual() > buf.length);
    assertEquals(1, (int) metaCache.activeSize());
    
    // Read again, now from data cache
    
    stats.reset();
    
    is = fs.open(newPath);
    is.readFully(buf);
    is.close();

    // Now we MUST read again everyting from write cache
    
    fromCache = stats.getTotalBytesReadDataCache();
    fromRemoteFS = stats.getTotalBytesReadRemote();
    fromWriteCache = stats.getTotalBytesReadWriteCache();
    
    // ALL reads must come from write cache
    assertEquals(0, (int) fromRemoteFS);
    assertEquals(buf.length,(int) fromCache);
    assertEquals(0, (int) fromWriteCache);
   
    // Delete file from write cache and data cache
    fst = fs.getFileStatus(newPath);
    sidecar.evictDataPages(newPath, fst);
    boolean result = sidecar.deleteFromWriteCache(newPath);
    assertTrue(result);
    
    stats.reset();
    // Read again, now from remote FS
    is = fs.open(newPath);
    is.readFully(buf);
    is.close();

    fromCache = stats.getTotalBytesReadDataCache();
    fromRemoteFS = stats.getTotalBytesReadRemote();
    fromWriteCache = stats.getTotalBytesReadWriteCache();
    
    // ALL reads must come from remote cache
    assertEquals(buf.length, (int) fromRemoteFS);
    assertEquals(0,(int) fromCache);
    assertEquals(0, (int) fromWriteCache);
    
    // Delete file completely
    
    result = fs.delete(newPath, true);
    assertTrue(result);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    assertEquals(0, (int) metaCache.activeSize());
    assertEquals(0, (int) dataCache.activeSize());

    assertFalse(fs.exists(newPath));
    assertFalse(writeCacheFS.exists(newCachedPath));
    fs.close();
  }
  
  @Test
  public void testDeleteWriteCachedFileWhileReading() throws IOException {
    LOG.info("Test Create - Delete file while reading data");
    Path workingDir = fs.getWorkingDirectory();
    Path p = new Path(workingDir, "testfile");
    FSDataOutputStream os = fs.create(p, true);
    
    byte[] buf = new byte[1 << 18];// 256K, 4 data pages
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
    // First call must return CachedFileStatus
    assertTrue(fst instanceof CachedFileStatus);
    assertEquals(buf.length, fst.getLen());
    
    SidecarCachingFileSystem sidecar = ((RemoteFileSystemAccess) fs).getCachingFileSystem();
    FileSystem writeCacheFS = sidecar.getWriteCacheFS();
    Path cachedPath = sidecar.remoteToCachingPath(p);
    fst = writeCacheFS.getFileStatus(cachedPath);
    assertEquals(buf.length, fst.getLen());
    
    
    // Now read data first time
    Statistics stats = sidecar.getStatistics();
    stats.reset();
    
    byte[] bbuf = new byte[buf.length];
    FSDataInputStream is = fs.open(p);
    // Read first half
    int read = is.read(bbuf, 0, bbuf.length / 2);
    // Verify that all reads came from write cache
    long fromCache = stats.getTotalBytesReadDataCache();
    long fromRemoteFS = stats.getTotalBytesReadRemote();
    long fromWriteCache = stats.getTotalBytesReadWriteCache();
    // ALL reads must come from write cache
    assertEquals(0, (int) fromRemoteFS);
    assertEquals(0, (int) fromCache);
    // We read the whole io buffer, because its sequential access
    assertEquals(ioBufferSize, (int) fromWriteCache);
    // Delete cached file
    sidecar.evictDataPages(p, fst);
    boolean result = sidecar.deleteFromWriteCache(p);
    assertTrue(result);
    // Reset counters
    stats.reset();
    // Read the rest of data from io buffer
    int readRest = is.read(bbuf, read, bbuf.length - read);
    is.close();
    
    fromCache = stats.getTotalBytesReadDataCache();
    fromRemoteFS = stats.getTotalBytesReadRemote();
    fromWriteCache = stats.getTotalBytesReadWriteCache();
    long fromPrefetch = stats.getTotalBytesReadPrefetch();
    // ALL reads must come from write cache
    assertEquals(readRest, (int) fromPrefetch);
    assertEquals(0, (int) fromCache);
    assertEquals(0, (int) fromWriteCache);
    assertEquals(0, (int) fromRemoteFS);
    assertEquals(buf.length, read + readRest);
    assertEquals(0, Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length));
    fs.close();
  }
  
  @Test
  public void testSaveLoad() throws IOException{
    LOG.info("Test Create - Read - Save - Load - Read ");
    Path workingDir = fs.getWorkingDirectory();
    Path p = new Path(workingDir, "testfile");
    FSDataOutputStream os = fs.create(p);
    
    byte[] buf = new byte[1 << 18];// 256K, 4 data pages
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
    // First call must return CachedFileStatus
    assertTrue(fst instanceof CachedFileStatus);
    assertEquals(buf.length, fst.getLen());
    
    SidecarCachingFileSystem sidecar = ((RemoteFileSystemAccess) fs).getCachingFileSystem();
    FileSystem writeCacheFS = sidecar.getWriteCacheFS();
    Path cachedPath = sidecar.remoteToCachingPath(p);
    fst = writeCacheFS.getFileStatus(cachedPath);
    assertEquals(buf.length, fst.getLen());
    
    Statistics stats = sidecar.getStatistics();
    stats.reset();
    // Now read data first time
    byte[] bbuf = new byte[buf.length];
    FSDataInputStream is = fs.open(p);
    // Read data - fill the data page cache
    is.read(bbuf, 0, bbuf.length);
    // Verify that all reads came from write cache
    long fromCache = stats.getTotalBytesReadDataCache();
    long fromRemoteFS = stats.getTotalBytesReadRemote();
    long fromWriteCache = stats.getTotalBytesReadWriteCache();
    // ALL reads must come from data write cache
    assertEquals(0, (int) fromRemoteFS);
    assertEquals(0, (int) fromCache);
    assertEquals(bbuf.length, (int) fromWriteCache);
    
    Cache dataCache = SidecarCachingFileSystem.getDataCache();
    Cache metaCache = SidecarCachingFileSystem.getMetaCache();
    
    assertEquals(1, metaCache.activeSize());
    long dataCacheSize = dataCache.getStorageUsedActual();
    
    sidecar.shutdown(false);
    SidecarCachingFileSystem.clear();
    // shutdown FS
    FileSystem.closeAll();
    
    try {
      this.fs = cachingFileSystem();
    } catch (Exception e) {
      LOG.error("setUp", e);
      fail();
    } 
    sidecar = ((RemoteFileSystemAccess) fs).getCachingFileSystem();
    stats = sidecar.getStatistics();
    stats.reset();
    is = fs.open(p);
    // Read data - fill the data page cache
    int read = is.read(bbuf, 0, bbuf.length);
    // Verify that all reads came from write cache
    
    fromCache = stats.getTotalBytesReadDataCache();
    fromRemoteFS = stats.getTotalBytesReadRemote();
    fromWriteCache = stats.getTotalBytesReadWriteCache();
    
    // ALL reads must come from data page cache
    assertEquals(0, (int) fromRemoteFS);
    assertEquals(0, (int) fromWriteCache);
    assertEquals(bbuf.length, (int) fromCache);

    
    dataCache = SidecarCachingFileSystem.getDataCache();
    metaCache = SidecarCachingFileSystem.getMetaCache();
    
    assertEquals(1, metaCache.activeSize());
    assertEquals(dataCacheSize, dataCache.getStorageUsedActual());
  }
}
