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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import com.carrot.sidecar.util.SidecarConfig;

public class TestSidecarCachingOutputStream {
  
  private static final Logger LOG = LoggerFactory.getLogger(TestSidecarCachingOutputStream.class);

  private static URI extDirectory;
  
  private URI cacheDirectory;
  
  private URI writeCacheDirectory;
  
  private long dataCacheSize = 400 * (1 << 20); // 400 MB
  
  private long dataCacheSegmentSize = 20 * (1 << 20);
  
  private long metaCacheSize = 400 * (1 << 20); // 400 MB
  
  private long metaCacheSegmentSize = 4 * (1 << 20);
  
  private long writeCacheSize = 2 * (1 << 20);
  
  int pageSize;
  
  int ioBufferSize;
  
  SidecarCachingFileSystem fs;
  
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
      this.fs = cachingFileSystem(true);
    } catch (Exception e) {
      LOG.error("setUp", e);
      fail();
    } 
  }
  
  @After 
  public void close() throws IOException {
    if (skipTests) return;
    SidecarCachingFileSystem.dispose();
    checkState(cacheDirectory != null);
    TestUtils.deletePathRecursively(cacheDirectory.getPath());
    LOG.info("Deleted {}", cacheDirectory);
    checkState(writeCacheDirectory != null);
    TestUtils.deletePathRecursively(writeCacheDirectory.getPath());
    LOG.info("Deleted {}", writeCacheDirectory);
  }
  
  private SidecarCachingFileSystem cachingFileSystem(boolean useWriteCache)
      throws URISyntaxException, IOException {
    
    SidecarConfig cacheConfig = SidecarConfig.getInstance();
    cacheConfig.setJMXMetricsEnabled(false);
    cacheConfig.setWriteCacheEnabled(useWriteCache);
    cacheConfig.setTestMode(true); // do not install shutdown hooks
    if (useWriteCache) {
      cacheConfig.setWriteCacheSizePerInstance(writeCacheSize);
      cacheConfig.setWriteCacheURI(writeCacheDirectory);
    }
    
    CarrotConfig carrotCacheConfig = CarrotConfig.getInstance();
    
    carrotCacheConfig.setGlobalCacheRootDir(cacheDirectory.getPath());
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.DATA_CACHE_NAME, dataCacheSize);
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.META_CACHE_NAME, metaCacheSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.DATA_CACHE_NAME, dataCacheSegmentSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.META_CACHE_NAME, metaCacheSegmentSize);
    
    Configuration configuration = TestUtils.getHdfsConfiguration(cacheConfig, carrotCacheConfig);
    String disableCacheName = "fs.file.impl.disable.cache";
    
    configuration.setBoolean(disableCacheName, true);
    
    FileSystem testingFileSystem = FileSystem.get(extDirectory, configuration);
    testingFileSystem.setWorkingDirectory(new Path(extDirectory.toString()));
    URI uri = new URI("carrot://test:8020/");
    // 
    SidecarCachingFileSystem cachingFileSystem = SidecarCachingFileSystem.get(testingFileSystem);
    cachingFileSystem.initialize(uri, configuration);
    
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
  public void testFileSystemWrite() throws Exception {
    Path basePath = fs.getRemoteFS().getWorkingDirectory();
    int num = 100;
    for (int i = 0; i < num; i++) {
      Path p = new Path(basePath, Integer.toString(i));
      FSDataOutputStream os =
          fs.create(p, null, true, 4096, (short) 1, dataCacheSegmentSize, null);

      int size = 1 << 16; // 64 KB
      byte[] buf = new byte[size];
      Random r = new Random();
      r.nextBytes(buf);

      os.write(buf);
      os.close();
      // Wait a bit
      Thread.sleep(100);
      // Read from caching FS
      FileSystem cachingFS = fs.getWriteCacheFS();
      FileSystem remoteFS = fs.getRemoteFS();
      Path cachePath = fs.remoteToCachingPath(p);
      FSDataInputStream cis = cachingFS.open(cachePath);
      byte[] bbuf = new byte[size];
      cis.readFully(bbuf);
      assertTrue(Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length) == 0);
      FSDataInputStream ris = remoteFS.open(p);
      ris.readFully(bbuf);
      assertTrue(Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length) == 0);
      FSDataInputStream sis = fs.open(p, 4096);
      sis.readFully(bbuf);
      assertTrue(Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length) == 0);
      // Close ALL
      cis.close();
      ris.close();
      sis.close();
      os.close();
    }
    // Last X files must be present in cache-on-write FS
    FileSystem cachingFS = fs.getWriteCacheFS();

    int n = 28;
    int total = 0;
    for (int i = num - 1; i >= 0; i--) {
      Path p = new Path(basePath, Integer.toString(i));
      p = fs.remoteToCachingPath(p);
      try {
        cachingFS.getFileStatus(p);
        total ++;
      } catch(FileNotFoundException e) {
        assertTrue(i < num - n);
      }
    }
    
    assertEquals(n, total);
    
    Cache cache = SidecarCachingFileSystem.getDataCache();
    Cache metaCache = SidecarCachingFileSystem.getMetaCache();
    assertTrue(metaCache.size() > 0);
    assertTrue(cache.size() > 0);
    
    for (int i = 0; i < num; i++) {
      Path p = new Path(basePath, Integer.toString(i));
      boolean result = fs.delete(p, false);
      assertTrue(result);
    }

    // wait a bit after delete
    Thread.sleep(1000);
    assertTrue(cache.activeSize() == 0);
    assertTrue(metaCache.activeSize() == 0);
  }
}
