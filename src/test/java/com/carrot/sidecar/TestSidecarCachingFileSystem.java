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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
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
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
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
import com.carrot.sidecar.util.LRUCache;
import com.carrot.sidecar.util.SidecarConfig;
import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Output;

public class TestSidecarCachingFileSystem {
  
  private static final Logger LOG = LoggerFactory.getLogger(TestSidecarCachingFileSystem.class);

  private static File sourceFile;

  private static URI extDirectory;
  
  private static long fileSize = 1L * (1 << 20);

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
    
  static boolean skipTests = false;
  
  @BeforeClass
  public static void setupClass() throws IOException {
    if (Utils.getJavaVersion() < 11) {
      skipTests = true;
      LOG.warn("Java 11+ is required to run test");
      return;
    }
    extDirectory = createTempDirectory("ext").toUri();
    sourceFile = TestUtils.createTempFile(extDirectory.getPath());
    TestUtils.fillRandom(sourceFile, fileSize);
  }
  
  @AfterClass
  public static void tearDown() throws IOException {
    if (skipTests) return;
    sourceFile.delete();
    TestUtils.deletePathRecursively(extDirectory.getPath());
    LOG.info("Deleted {}", sourceFile.getAbsolutePath());
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
      e.printStackTrace();
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
  
  @Test 
  public void testPathConversions() {
    Path extPathBase = new Path(extDirectory.toString());
    Path extPath1 = new Path(extPathBase, "dir1");
    Path extPath2 = new Path(extPath1, "file1");
    Path cachePath1 = fs.remoteToCachingPath(extPath1);
    Path cachePath2 = fs.remoteToCachingPath(extPath2);
    
    LOG.info(" ext1 = {} cache1 = {}", extPath1, cachePath1);
    LOG.info(" ext2 = {} cache2 = {}", extPath2, cachePath2);
    LOG.info("");
  }
  
  @Test
  public void testFileStatus() throws IOException {
    FsPermission perm = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
    String user = "user";
    String group = "group";
    if (sourceFile == null) {
      sourceFile = TestUtils.createTempFile();
    }
    FileStatus fs = new FileStatus(100000000, false, 3, 64000000, System.currentTimeMillis(),
      System.currentTimeMillis(), perm, user, group,
      new Path(sourceFile.toURI()));
    
    Kryo kryo = new Kryo();
    kryo.register(FileStatus.class);
    kryo.register(Path.class);
    kryo.register(URI.class);
    kryo.register(FsPermission.class);
    kryo.register(FsAction.class);
    Output out = new Output(4096);
    kryo.writeObject(out, fs);
    int size = out.position();
    LOG.info("FileStatus size={} path len={}", size, sourceFile.toString().length());  
  }
  
  @Test
  public void testFileSystemRead() throws Exception {
    if (skipTests) return;

    FSDataInputStream extStream = this.fs.getRemoteFS().open(new Path(sourceFile.getAbsolutePath()));
    int len = 1000;
    byte[] buffer = new byte[len];
    byte[] compBuffer = new byte[len];
    
    Random r = new Random();
    
    for (int i = 0; i < 1024; i++) {
      long pos = r.nextLong();
      long off = Math.abs(pos) % (fileSize - len);
      
      extStream.readFully(off, compBuffer, 0, len);
      int read = readFully(off, buffer, 0, len);
      assertTrue(buffer.length == read);
      boolean res = Utils.compareTo(buffer, 0, len, compBuffer, 0, len) == 0;
      if (!res) {
        System.out.println(Utils.toHex(compBuffer, 0, len));
        System.out.println(Utils.toHex(buffer, 0, len));
      }
      assertTrue(res);
    }
  }
  
  private void monikerExists(Path p) throws Exception {
    Path moniker = monikerPath(p);
    FileSystem writeCacheFS = fs.getWriteCacheFS();
    assertTrue(writeCacheFS.exists(moniker));
  }
  
  private void monikerDoesNotExists(Path p) throws Exception {
    Path moniker = monikerPath(p);
    FileSystem writeCacheFS = fs.getWriteCacheFS();
    assertFalse(writeCacheFS.exists(moniker));
  }
  
  private Path monikerPath(Path p) {
    p = fs.remoteToCachingPath(p);
    return new Path(p.toString() + ".toupload");
  }
  
  @Test
  public void testFileSystemCreateAndDelete() throws Exception {
    FileSystem remoteFS = fs.getRemoteFS();
    Path workDir = remoteFS.getWorkingDirectory();
    Path p = new Path(workDir, "test-file");

    FSDataOutputStream os = fs.create(p, null, true, 4096, (short)1, dataCacheSegmentSize, null);
    LOG.info("remote working directory {}", fs.getRemoteFS().getWorkingDirectory());
    LOG.info("write c working directory {}", fs.getWriteCacheFS().getWorkingDirectory());
    
    monikerExists(p);
    
    int size = 1 << 20;
    byte[] buf = new byte[size];
    Random r = new Random();
    r.nextBytes(buf);
    
    os.write(buf);
    os.close();
    // Wait a bit
    Thread.sleep(1000);
    // Read from caching FS
    FileSystem cachingFS = fs.getWriteCacheFS();
    Path cachePath = fs.remoteToCachingPath(p);
    FSDataInputStream cis = cachingFS.open(cachePath);
    byte[] bbuf = new byte[size];
    cis.readFully(bbuf);
    assertTrue(Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length) == 0);
    FSDataInputStream ris = remoteFS.open(p); 
    ris.readFully(bbuf);
    assertTrue(Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length) == 0);
    
    // now read using sidecar FS - populate data cache
    FSDataInputStream sid = fs.open(p, size);
    sid.readFully(bbuf);
    assertTrue(Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length) == 0);

    Cache cache = SidecarCachingFileSystem.getDataCache();
    Cache metaCache = SidecarCachingFileSystem.getMetaCache();
    assertTrue(metaCache.size() == 1);
    assertTrue(cache.size() == 1);
    
    fs.delete(p, true);
    // wait a bit after delete
    Thread.sleep(1000);
    
    monikerDoesNotExists(p);
    
    // Little trick to get right size
    assertTrue(cache.activeSize() == 0);
    assertTrue(metaCache.activeSize() == 0);
    
  }
  
  @Test
  public void testFileSystemSaveAndLoad() throws Exception {
    FileSystem remoteFS = fs.getRemoteFS();
    Path workDir = remoteFS.getWorkingDirectory();
    Path p = new Path(workDir, "test-file");

    FSDataOutputStream os = fs.create(p, null, true, 4096, (short)1, dataCacheSegmentSize, null);
    LOG.info("remote working directory {}", fs.getRemoteFS().getWorkingDirectory());
    LOG.info("write c working directory {}", fs.getWriteCacheFS().getWorkingDirectory());
    
    monikerExists(p);
    
    int size = 1 << 20;
    byte[] buf = new byte[size];
    Random r = new Random();
    r.nextBytes(buf);
    
    os.write(buf);
    os.close();
    // Wait a bit
    Thread.sleep(1000);
    byte[] bbuf = new byte[size];

    // now read using sidecar FS - populate data cache
    FSDataInputStream sid = fs.open(p, size);
    sid.readFully(bbuf);
    assertTrue(Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length) == 0);
    
    sid.close();
    
    Cache cache = SidecarCachingFileSystem.getDataCache();
    Cache metaCache = SidecarCachingFileSystem.getMetaCache();
    LRUCache<String, Long> fifoCache = SidecarCachingFileSystem.getLRUCache();
    
    assertTrue(metaCache.size() == 1);
    assertTrue(cache.size() == 1);
    assertTrue(fifoCache.size() == 1);
    
    assertTrue(metaCache.exists(p.toString().getBytes()));
    Path pp = fs.remoteToCachingPath(p);
    assertTrue(fifoCache.get(pp.toString()) != null);
    
    // shutdown Sidecar - this will 
    fs.shutdown();
    
    // Initialize Sidecar again
    fs = cachingFileSystem(true);
    
    cache = SidecarCachingFileSystem.getDataCache();
    metaCache = SidecarCachingFileSystem.getMetaCache();
    fifoCache = SidecarCachingFileSystem.getLRUCache();
    assertTrue(metaCache.size() == 1);
    assertTrue(cache.size() == 1);
    assertTrue(fifoCache.size() == 1);
    
    assertTrue(metaCache.exists(p.toString().getBytes()));
    pp = fs.remoteToCachingPath(p);
    assertTrue(fifoCache.get(pp.toString()) != null);
    
    // now read using sidecar FS - populate data cache
    sid = fs.open(p, size);
    sid.readFully(bbuf);
    assertTrue(Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length) == 0);
    
  }
  
  @Test
  public void testFileSystemRename() throws Exception {
    FileSystem remoteFS = fs.getRemoteFS();
    Path workDir = remoteFS.getWorkingDirectory();
    Path p = new Path(workDir, "test-file");

    FSDataOutputStream os = fs.create(p, null, true, 4096, (short)1, dataCacheSegmentSize, null);
    LOG.info("remote working directory {}", fs.getRemoteFS().getWorkingDirectory());
    LOG.info("write c working directory {}", fs.getWriteCacheFS().getWorkingDirectory());
    
    monikerExists(p);
    
    int size = 1 << 20;
    byte[] buf = new byte[size];
    Random r = new Random();
    r.nextBytes(buf);
    
    os.write(buf);
    os.close();
    // Wait a bit
    Thread.sleep(1000);
    byte[] bbuf = new byte[size];

    // now read using sidecar FS - populate data cache
    FSDataInputStream sid = fs.open(p, size);
    sid.readFully(bbuf);
    assertTrue(Utils.compareTo(buf, 0, buf.length, bbuf, 0, bbuf.length) == 0);
    
    Cache cache = SidecarCachingFileSystem.getDataCache();
    Cache metaCache = SidecarCachingFileSystem.getMetaCache();
    assertTrue(metaCache.size() == 1);
    assertTrue(cache.size() == 1);
    
    Path dst = new Path(workDir, "test-file1");
    fs.rename(p, dst);
    
    Thread.sleep(1000);
        
    assertFalse(remoteFS.exists(p));
    assertTrue (remoteFS.exists(dst));
    
    Path cacheSrc = fs.remoteToCachingPath(p);
    Path cacheDst = fs.remoteToCachingPath(dst);
    
    FileSystem cacheFS = fs.getWriteCacheFS();
    assertFalse(cacheFS.exists(cacheSrc));
    assertTrue (cacheFS.exists(cacheDst));
    
    assertTrue(cache.activeSize() == 0);
    // We removed src from meta, but added dst - so still size is 1
    assertTrue(metaCache.activeSize() == 1);
  }
  
  public void testFileSystemAppend() throws Exception {
    //TODO - append is not supported by object stores
  }
  
  private int readFully(long position, byte[] buffer, int offset, int length) throws Exception {

    try (FSDataInputStream stream = fs.open(new Path(sourceFile.getAbsolutePath()), 1 << 20)) {
      int read = 0;
      while (read < length) {
        read += stream.read(position + read, buffer, offset + read, length - read);
      }
      return read;
    }
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
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.DATA_CACHE_FILE_NAME, dataCacheSize);
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.META_CACHE_NAME, metaCacheSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.DATA_CACHE_FILE_NAME, dataCacheSegmentSize);
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
  
}
