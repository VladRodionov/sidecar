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
package com.carrotdata.sidecar;

import static java.nio.file.Files.createTempDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import com.carrotdata.sidecar.RemoteFileSystemAccess;
import com.carrotdata.sidecar.SidecarCachingFileSystem;
import com.carrotdata.sidecar.SidecarConfig;
import com.carrotdata.sidecar.SidecarDataCacheType;
import com.carrotdata.sidecar.WriteCacheMode;
import com.carrotdata.sidecar.fs.file.SidecarLocalFileSystem;

public class TestSidecarCachingOutputStream {
  
  private static final Logger LOG = LoggerFactory.getLogger(TestSidecarCachingOutputStream.class);

  private static URI extDirectory;
  
  private URI cacheDirectory;
  
  private URI writeCacheDirectory;
  
  private long dataCacheSize = 400 * (1 << 20); // 400 MB
  
  private long dataCacheSegmentSize = 20 * (1 << 20);
  
  private long metaCacheSize = 400 * (1 << 20); // 400 MB
  
  private long metaCacheSegmentSize = 4 * (1 << 20);
  
  private long writeCacheSize = 20 * (1 << 20);
  
  int pageSize;
  
  int ioBufferSize;
  
  SidecarCachingFileSystem sidecar;
  FileSystem testFS;
  
  static boolean skipTests ;
  
  @BeforeClass
  public static void setupClass() throws IOException {
    if (Utils.getJavaVersion() < 11) {
      skipTests = true;
      LOG.warn("Java 11+ is required to run test");
      return;
    }
    extDirectory = createTempDirectory("ext").toUri();
    
  }
  
  @AfterClass
  public static void tearDown() throws IOException {
    if (skipTests) return;
    TestUtils.deletePathRecursively(extDirectory.getPath());
    LOG.info("Deleted {}", extDirectory.getPath());
  }
  
  @Before
  public void setUp() throws IOException {
    if (skipTests) {
      return;
    }
    this.cacheDirectory = createTempDirectory("carrot_cache").toUri();
    this.writeCacheDirectory = createTempDirectory("write_cache").toUri();
    try {
      this.sidecar = cachingFileSystem(true);
    } catch (Exception e) {
      LOG.error("setUp", e);
      fail();
    } 
  }
  
  @After 
  public void close() throws IOException {
    if (skipTests) return;
    SidecarCachingFileSystem.dispose();
    TestUtils.deletePathRecursively(cacheDirectory.getPath());
    LOG.info("Deleted {}", cacheDirectory);
    TestUtils.deletePathRecursively(writeCacheDirectory.getPath());
    LOG.info("Deleted {}", writeCacheDirectory);
  }
  
  private SidecarCachingFileSystem cachingFileSystem(boolean useWriteCache)
      throws URISyntaxException, IOException {
    
    SidecarConfig cacheConfig = SidecarConfig.getInstance();
    cacheConfig.setJMXMetricsEnabled(false);
    cacheConfig.setWriteCacheMode(useWriteCache? WriteCacheMode.ASYNC_CLOSE: WriteCacheMode.DISABLED);
    cacheConfig.setTestMode(true); // do not install shutdown hooks
    cacheConfig.setDataCacheType(SidecarDataCacheType.FILE);
    if (useWriteCache) {
      cacheConfig.setWriteCacheSizePerInstance(writeCacheSize);
      cacheConfig.setWriteCacheURI(writeCacheDirectory);
    }
    
    CacheConfig carrotCacheConfig = CacheConfig.getInstance();
    
    carrotCacheConfig.setGlobalCacheRootDir(cacheDirectory.getPath());
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.DATA_CACHE_FILE_NAME, dataCacheSize);
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.META_CACHE_NAME, metaCacheSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.DATA_CACHE_FILE_NAME, dataCacheSegmentSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.META_CACHE_NAME, metaCacheSegmentSize);
    
    Configuration configuration = TestUtils.getHdfsConfiguration(cacheConfig, carrotCacheConfig);
    
    configuration.set("fs.file.impl", SidecarLocalFileSystem.class.getName());
    configuration.set("fs.file.impl.disable.cache", Boolean.TRUE.toString());
    //configuration.set(SidecarConfig.SIDECAR_WRITE_CACHE_ENABLED_KEY, Boolean.TRUE.toString());
    configuration.set(SidecarConfig.SIDECAR_WRITE_CACHE_SIZE_KEY, Long.toString(writeCacheSize));
    configuration.set(SidecarConfig.SIDECAR_WRITE_CACHE_URI_KEY, writeCacheDirectory.toString());
    configuration.set(SidecarConfig.SIDECAR_TEST_MODE_KEY, Boolean.TRUE.toString());
    
    testFS = FileSystem.get(extDirectory, configuration);
    testFS.setWorkingDirectory(new Path(extDirectory.toString()));
    // 
    SidecarCachingFileSystem cachingFileSystem = 
        ((RemoteFileSystemAccess) testFS).getCachingFileSystem();
    
    cachingFileSystem.setMetaCacheEnabled(true);
    // Verify initialization
    Cache dataCache = SidecarCachingFileSystem.getDataCache();
    assertEquals(dataCacheSize, dataCache.getMaximumCacheSize());
    assertEquals(dataCacheSegmentSize, dataCache.getEngine().getSegmentSize());
    Cache metaCache = SidecarCachingFileSystem.getMetaCache();
    assertEquals(metaCacheSize, metaCache.getMaximumCacheSize());
    assertEquals(metaCacheSegmentSize, metaCache.getEngine().getSegmentSize());
    
    return cachingFileSystem;
  }
  
  @Test
  public void testFileSystemWriteDelete() throws Exception {
    Path basePath = testFS.getWorkingDirectory();
    Cache cache = SidecarCachingFileSystem.getDataCache();
    Cache metaCache = SidecarCachingFileSystem.getMetaCache();
    int num = 1000;
    for (int i = 0; i < num; i++) {
      Path p = new Path(basePath, Integer.toString(i));
      FSDataOutputStream os =
          testFS.create(p, null, true, 4096, (short) 1, dataCacheSegmentSize, null);

      int size = 1 << 16; // 64 KB
      byte[] buf = new byte[size];
      Random r = new Random();
      r.nextBytes(buf);

      os.write(buf);
      os.close();
      
      // Wait a bit
      // Read from caching FS
      FileSystem cachingFS = sidecar.getWriteCacheFS();
      FileSystem remoteFS = sidecar.getRemoteFS();
      Path cachePath = sidecar.remoteToCachingPath(p);
      FSDataInputStream cis = cachingFS.open(cachePath);
      byte[] bbuf = new byte[size];
      cis.readFully(bbuf);
      assertTrue(Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length) == 0);
      FSDataInputStream ris = remoteFS.open(p);
      ris.readFully(bbuf);
      assertTrue(Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length) == 0);
      FSDataInputStream sis = sidecar.open(p, 4096);
      sis.readFully(bbuf);
      assertTrue(Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length) == 0);
      // Close ALL
      cis.close();
      ris.close();
      sis.close();
    }
    Thread.sleep(1000);
    
    // Last X files must be present in cache-on-write FS
    // Verify that no moniker files present in write caching FS

    FileSystem cachingFS = sidecar.getWriteCacheFS();    
    FileStatus[] fss = cachingFS.listStatus(new Path(this.writeCacheDirectory.toString()), x -> 
        x.toString().endsWith(".toupload")
    );
    
    assertTrue(fss.length == 0);
    
    int n = 303; // we expect only 303 files left in a write cache FS
    int total = 0;
    for (int i = num - 1; i >= 0; i--) {
      Path p = new Path(basePath, Integer.toString(i));
      p = sidecar.remoteToCachingPath(p);
      try {
        cachingFS.getFileStatus(p);
        total ++;
      } catch(FileNotFoundException e) {
        assertTrue(i < num - n);
      }
    }
    
    assertEquals(n, total);
    assertEquals(num, metaCache.activeSize());
    assertTrue(cache.size() > 0);
    
    for (int i = 0; i < num; i++) {
      Path p = new Path(basePath, Integer.toString(i));
      boolean result = sidecar.delete(p, false);
      assertTrue(result);
    }
    // wait a bit after delete
    Thread.sleep(1000);
    // Verify that  write cache directory is empty
    
    assertTrue(cache.activeSize() == 0);
    assertTrue(metaCache.activeSize() == 0);
  }
  
  @Test
  public void testFileSystemWriteRename() throws Exception {
    Path basePath = testFS.getWorkingDirectory();
    int num = 100;
    for (int i = 0; i < num; i++) {
      Path p = new Path(basePath, Integer.toString(i));
      FSDataOutputStream os =
          testFS.create(p, null, true, 4096, (short) 1, dataCacheSegmentSize, null);

      int size = 1 << 16; // 64 KB
      byte[] buf = new byte[size];
      Random r = new Random();
      r.nextBytes(buf);

      os.write(buf);
      os.close();
      // Wait a bit
      // Read from caching FS
      FileSystem cachingFS = sidecar.getWriteCacheFS();
      FileSystem remoteFS = sidecar.getRemoteFS();
      Path cachePath = sidecar.remoteToCachingPath(p);
      FSDataInputStream cis = cachingFS.open(cachePath);
      byte[] bbuf = new byte[size];
      cis.readFully(bbuf);
      assertTrue(Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length) == 0);
      FSDataInputStream ris = remoteFS.open(p);
      ris.readFully(bbuf);
      assertTrue(Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length) == 0);
      FSDataInputStream sis = sidecar.open(p, 4096);
      sis.readFully(bbuf);
      assertTrue(Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length) == 0);
      // Close ALL
      cis.close();
      ris.close();
      sis.close();
      os.close();
    }
    Thread.sleep(1000);
    // Last X files must be present in cache-on-write FS
    FileSystem cachingFS = sidecar.getWriteCacheFS();

    for (int i = num - 1; i >= 0; i--) {
      Path p = new Path(basePath, Integer.toString(i));
      p = sidecar.remoteToCachingPath(p);
      try {
        cachingFS.getFileStatus(p);
      } catch(FileNotFoundException e) {
        fail();
      }
      // OK, file exists in the write cache
      p = new Path(basePath, Integer.toString(i));
      Path newPath = new Path(p.toString() + "x");
      boolean b = sidecar.rename(p, newPath);
      assertTrue(b);
      //
      p = sidecar.remoteToCachingPath(p);
      try {
        // does not exists
        cachingFS.getFileStatus(p);
        fail();
      } catch(FileNotFoundException e) {
      }
      // new path must exists
      p = sidecar.remoteToCachingPath(newPath);
      try {
        // does  exists
        cachingFS.getFileStatus(p);
      } catch(FileNotFoundException e) {
        fail();
      }
    }
    
    Cache cache = SidecarCachingFileSystem.getDataCache();
    Cache metaCache = SidecarCachingFileSystem.getMetaCache();
    assertTrue(metaCache.activeSize() == num);
    assertTrue(cache.size() > 0);
    
    for (int i = 0; i < num; i++) {
      Path p = new Path(basePath, Integer.toString(i));
      Path newPath = new Path(p.toString() + "x");

      boolean result = sidecar.delete(newPath, false);
      assertTrue(result);
    }

    // wait a bit after delete
    Thread.sleep(1000);
    assertTrue(cache.activeSize() == 0);
    assertTrue(metaCache.activeSize() == 0);
  }
}
