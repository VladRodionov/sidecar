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

import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrot.cache.Builder;
import com.carrot.cache.Cache;
import com.carrot.cache.ObjectCache;
import com.carrot.cache.controllers.LRCRecyclingSelector;
import com.carrot.cache.index.CompactBaseIndexFormat;
import com.carrot.cache.io.BaseDataWriter;
import com.carrot.cache.io.BaseFileDataReader;
import com.carrot.cache.io.BaseMemoryDataReader;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.Utils;
import com.carrot.sidecar.util.FIFOCache;
import com.google.common.annotations.VisibleForTesting;

public class SidecarCachingFileSystem implements SidecarCachingOutputStream.Listener{
  
  private static final Logger LOG = LoggerFactory.getLogger(SidecarCachingFileSystem.class);

  private final static String DATA_CACHE_NAME = "sidecar-data";
  private final static String META_CACHE_NAME = "sidecar-meta";
  /*
   *  Local cache for remote file data
   */
  private static Cache dataCache; // singleton
  /*
   *  Caches remote file lengths by file path
   */
  private static ObjectCache metaCache; // singleton

  /*
   *  FIFO cache for cached on write filenames with their lengths (if enabled) 
   */
  private static FIFOCache<String, Long> writeCacheFileList;
  
  private static ConcurrentHashMap<String, SidecarCachingFileSystem> cachedFS =
      new ConcurrentHashMap<>();

  /*
   * Thread pool
   */
  private ExecutorService unboundedThreadPool;
  
  /*
   *  Data page size for data cache 
   */
  volatile private int dataPageSize;
  
  /*
   *  I/O buffer size for data cache 
   */
  volatile private int ioBufferSize;
  
  /*
   *  I/O pool size for data cache 
   */
  volatile private int ioPoolSize;
  
  /*
   * Initialization was done
   */
  volatile private boolean inited = false;
  
  /*
   * Remote file system
   * TODO: multiple remote FS support
   */
  private FileSystem remoteFS;
  
  /* 
   * Cache-on-write file system 
   */
  private FileSystem writeCacheFS;
  
  /*
   *  Is cache-on-write enabled
   */
  private boolean writeCacheEnabled;
  
  /*
   *  Write cache maximum size (per server instance)
   */
  private long writeCacheMaxSize;
  
  /*
   *  Current write cache size 
   */
  AtomicLong writeCacheSize = new AtomicLong();
  /*
   * File eviction thread (from write cache)
   */
  AtomicReference<Thread> evictor;

  public static SidecarCachingFileSystem get(FileSystem dataTier) throws IOException {
    checkJavaVersion();

    String uri = dataTier.getUri().toString();
    SidecarCachingFileSystem fs = cachedFS.get(uri);
    if (fs == null) {
      synchronized (SidecarCachingFileSystem.class) {
        if (cachedFS.contains(uri)) {
          return cachedFS.get(uri);
        }
        fs = new SidecarCachingFileSystem(dataTier);
        cachedFS.put(uri, fs);
      }
    }
    return fs;
  }

  private static void checkJavaVersion() throws IOException {
    if (Utils.getJavaVersion() < 11) {
      throw new IOException("Java 11+ is required to run Sidecar FS.");
    }
  }

  private SidecarCachingFileSystem(FileSystem fs) {
    this.remoteFS = fs;
  }

  public void initialize(URI uri, Configuration configuration) throws IOException {
    try {
      if (inited) {
        return;
      }
      CarrotConfig config = CarrotConfig.getInstance();
      Iterator<Map.Entry<String, String>> it = configuration.iterator();
      while (it.hasNext()) {
        Map.Entry<String, String> entry = it.next();
        String name = entry.getKey();
        if (CarrotConfig.isCarrotPropertyName(name)) {
          config.setProperty(name, entry.getValue());
        }
      }
      
      SidecarConfig sconfig = SidecarConfig.fromHadoopConfiguration(configuration);
      
      this.dataPageSize = (int) sconfig.getDataPageSize();
      this.ioBufferSize = (int) sconfig.getIOBufferSize();
      this.ioPoolSize = (int) sconfig.getIOPoolSize();
      this.writeCacheEnabled = sconfig.isWriteCacheEnabled();
      if (this.writeCacheEnabled) {
        this.writeCacheMaxSize = sconfig.getWriteCacheSizePerInstance();
        URI writeCacheURI = sconfig.getWriteCacheURI();
        if (writeCacheURI != null) {
          this.writeCacheFS = FileSystem.get(writeCacheURI, configuration);
        } else {
          LOG.error("Write cache location is not specified. Disable cache on write");
        }
      }
      
      if (dataCache != null) {
        return;
      }

      synchronized (getClass()) {
        if (dataCache != null) {
          return;
        }
        SidecarCachingInputStream.initIOPools(this.ioPoolSize);
        loadDataCache();
        loadMetaCache();
        loadFIFOCache();
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          try {
            saveDataCache();
            saveMetaCache();
            saveFIFOCache();
          } catch (IOException e) {
            LOG.error(e.getMessage(), e);
          }
        }));
        LOG.info("Shutdown hook installed for cache[{}]", dataCache.getName());
      }
      this.inited = true;
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e);
    }
    //TODO: make it configurable
    int maxThreads = 8;
    int keepAliveTime = 60;
    unboundedThreadPool = new ThreadPoolExecutor(
      maxThreads, Integer.MAX_VALUE,
      keepAliveTime, TimeUnit.SECONDS,
      new LinkedBlockingQueue<Runnable>(),
      BlockingThreadPoolExecutorService.newDaemonThreadFactory(
          "sidecar-unbounded"));

  }

  private void loadFIFOCache() throws IOException {
    CarrotConfig config = CarrotConfig.getInstance();
    String snapshotDir = config.getSnapshotDir(FIFOCache.NAME);
    String fileName = snapshotDir + File.separator + FIFOCache.FILE_NAME;
    File file = new File(fileName);
    writeCacheFileList = new FIFOCache<>();

    if (file.exists()) {
      FileInputStream fis = new FileInputStream(file);
      writeCacheFileList.load(fis);
    } 
  }

  private void loadMetaCache() throws IOException{
    CarrotConfig config = CarrotConfig.getInstance();
    SidecarConfig sconfig = SidecarConfig.getInstance();
    try {
      String rootDir = config.getGlobalCacheRootDir();
      long maxSize = config.getCacheMaximumSize(META_CACHE_NAME);
      int dataSegmentSize = (int) config.getCacheSegmentSize(META_CACHE_NAME);
      metaCache = ObjectCache.loadCache(rootDir, META_CACHE_NAME);
      LOG.info("Loaded cache={} from={} name={}", metaCache, rootDir, META_CACHE_NAME);
      if (metaCache == null) {
        Builder builder = new Builder(META_CACHE_NAME);
        builder = builder
            
            .withCacheMaximumSize(maxSize)
            .withCacheDataSegmentSize(dataSegmentSize)
            .withRecyclingSelector(LRCRecyclingSelector.class.getName())
            .withDataWriter(BaseDataWriter.class.getName())
            .withMemoryDataReader(BaseMemoryDataReader.class.getName())
            .withFileDataReader(BaseFileDataReader.class.getName())
            .withMainQueueIndexFormat(CompactBaseIndexFormat.class.getName());       
        metaCache = builder.buildObjectMemoryCache();
      }
    } catch (IOException e) {
      LOG.error("loadMetaCache error", e);
      throw e;
    }  
    
    Class<?> keyClass = String.class;
    Class<?> valueClass = Long.class;
   
    metaCache.addKeyValueClasses(keyClass, valueClass);
    
    if (sconfig.isJMXMetricsEnabled()) {
      LOG.info("SidecarCachingFileSystem JMX enabled for meta-data cache");
      String domainName = config.getJMXMetricsDomainName();
      metaCache.registerJMXMetricsSink(domainName);
    }  
  }

  private void loadDataCache() throws IOException {
    CarrotConfig config = CarrotConfig.getInstance();
    try {
      dataCache = Cache.loadCache(DATA_CACHE_NAME);
      if (dataCache != null) {
        LOG.info("Loaded cache[{}] from the path: {}", dataCache.getName(),
          config.getCacheRootDir(dataCache.getName()));
      }
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }

    if (dataCache == null) {
      // Create new instance
      LOG.info("Creating new cache");
      dataCache = new Cache(DATA_CACHE_NAME, config);
      LOG.info("Created new cache[{}]", dataCache.getName());
    }
    LOG.info("Initialized cache[{}]", dataCache.getName());
    SidecarConfig sconfig = SidecarConfig.getInstance();
    boolean metricsEnabled = sconfig.isJMXMetricsEnabled();

    if (metricsEnabled) {
      String domainName = sconfig.getJMXMetricsDomainName();
      LOG.info("SidecarCachingFileSystem JMX enabled for data cache");
      dataCache.registerJMXMetricsSink(domainName);
    }
  }
  
  private void saveFIFOCache() throws IOException {
    if (writeCacheFileList != null) {
      long start = System.currentTimeMillis();
      LOG.info("Shutting down cache[{}]", FIFOCache.NAME);
      CarrotConfig config = CarrotConfig.getInstance();
      String snapshotDir = config.getSnapshotDir(FIFOCache.NAME);
      FileOutputStream fos = new FileOutputStream(snapshotDir + 
      File.separator + FIFOCache.FILE_NAME);
      writeCacheFileList.save(fos);
      // do not close - it was closed already
      long end = System.currentTimeMillis();
      LOG.info("Shutting down cache[{}] done in {}ms",FIFOCache.NAME , (end - start));
    }
  }

  private void saveDataCache() throws IOException {
    long start = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}]", DATA_CACHE_NAME);
    dataCache.shutdown();
    long end = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}] done in {}ms", DATA_CACHE_NAME, (end - start));
  }
  
  private void saveMetaCache() throws IOException {
    long start = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}]", META_CACHE_NAME);
    metaCache.shutdown();
    long end = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}] done in {}ms", META_CACHE_NAME, (end - start));
  }
  
  /**********************************************************************************
   * 
   * FileSystem API
   * 
   **********************************************************************************/
  
  /**
   * Open file 
   * @param path path to the file
   * @param bufferSize buffer size
   * @return input stream
   * @throws Exception
   */
  
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {

    LOG.debug("Open {}", path);
    
    Future<FSDataInputStream> remoteStreamFuture = null;
    Future<FSDataInputStream> cacheStreamFuture = null;
    
    Callable<FSDataInputStream> remoteCall = () -> {
      return remoteFS.open(path, bufferSize);
    };
    
    Callable<FSDataInputStream> cacheCall = () -> {
      return writeCacheFS == null? null: writeCacheFS.open(path, bufferSize);
    };
    
    remoteStreamFuture = unboundedThreadPool.submit(remoteCall);
    cacheStreamFuture = unboundedThreadPool.submit(cacheCall);
    
    Long fileLength = (Long) metaCache.get(path.toString()); 
    if (fileLength == null) {
      FileStatus fs = remoteFS.getFileStatus(path);
      fileLength = fs.getLen();
      metaCache.put(path.toString(), fileLength, 0 /* No expiration*/);
    }
    FSDataInputStream cachingInputStream =
        new FSDataInputStream(new SidecarCachingInputStream(dataCache, path, remoteStreamFuture, cacheStreamFuture, fileLength,
            dataPageSize, ioBufferSize));
    return cachingInputStream;
  }
  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param permission the permission to set.
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize the requested block size.
   * @param progress the progress reporter.
   * @throws IOException in the event of IO related errors.
   * @see #setPermission(Path, FsPermission)
   */
  @SuppressWarnings("deprecation")
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
  
    LOG.debug("Create {}", f);
    FSDataOutputStream remoteOut = 
        this.remoteFS.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    if (!this.writeCacheEnabled) {
      return remoteOut;
    }
    
    Path cachePath = remoteToCachingPath(f);
    FSDataOutputStream cacheOut = null;
    try {
        this.writeCacheFS.create(cachePath, overwrite, bufferSize, replication, blockSize);
    } catch (IOException e) {
      LOG.error("Write cache create file failed", e);
      return remoteOut;
    }

    // Create file moniker
    createMoniker(cachePath);
    return new FSDataOutputStream(new SidecarCachingOutputStream(cacheOut, remoteOut, f));
  }
  
  private void createMoniker(Path cachePath) throws IOException {
    Path p = new Path(cachePath.toString() + ".toupload");
    FSDataOutputStream os = this.writeCacheFS.create(p);
    os.close();
  }

  private void deleteMoniker(Path cachePath) throws IOException {
    Path p = new Path(cachePath.toString() + ".toupload");
    this.writeCacheFS.delete(p, false);
  }
  
  @SuppressWarnings("deprecation")
  public FSDataOutputStream createNonRecursive(Path path, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    
    LOG.debug("Create non-recursive {}", path);
    FSDataOutputStream remoteOut = 
        this.remoteFS.createNonRecursive(path, permission, flags, bufferSize, replication, blockSize, progress);

    if (!this.writeCacheEnabled) {
      return remoteOut;
    }
    
    Path cachePath = remoteToCachingPath(path);
    FSDataOutputStream cacheOut = null;
    try {
        this.writeCacheFS.create(cachePath, true, bufferSize, replication, blockSize);
    } catch(IOException e) {
      LOG.error("Write cache create file failed", e);
      return remoteOut;
    }
    
    // Create file moniker
    createMoniker(cachePath);
    return new FSDataOutputStream(new SidecarCachingOutputStream(cacheOut, remoteOut, path));
  }

  @SuppressWarnings("deprecation")
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    LOG.debug("Append {}", f);
    FSDataOutputStream remoteOut = this.remoteFS.append(f, bufferSize, progress);

    // Usually append is not supported by cloud object stores
    if (!this.writeCacheEnabled) {
      return remoteOut;
    }
    Path cachePath = remoteToCachingPath(f);
    
    FSDataOutputStream cacheOut = this.writeCacheFS.append(cachePath, bufferSize);
    return new FSDataOutputStream(new SidecarCachingOutputStream(cacheOut, remoteOut, f));
  }

  public boolean rename(Path src, Path dst) throws IOException {
    LOG.debug("Rename {} to {}", src, dst);

    if (!this.writeCacheEnabled) {
      return this.remoteFS.rename(src, dst);
    }
    boolean result = this.remoteFS.rename(src, dst);
    Path cacheSrc = null, cacheDst = null;
    try {
      cacheSrc = remoteToCachingPath(src);
      cacheDst = remoteToCachingPath(dst);
      this.writeCacheFS.rename(cacheSrc, cacheDst);
    } catch (IOException e) {
      LOG.error("Failed to rename {} to {}", cacheSrc, cacheDst);
    }
   
    return result;
  }

  public boolean delete(Path f, boolean recursive) throws IOException {
    
    LOG.debug("Delete {} recursive={}", f, recursive);
    
    if (!this.writeCacheEnabled) {
      return this.remoteFS.delete(f, recursive);
    }
    
    boolean result = this.remoteFS.delete(f, recursive);
    Path p = remoteToCachingPath(f);
    try {
      this.writeCacheFS.delete(p, recursive);
    } catch (IOException e) {
      //TODO
    }
    // Check if f is file
    boolean isFile = metaCache.exists(f.toString());
    if (isFile) {
      // Clear data pages cache
      Runnable r = () -> {
        evictDataPages(f);
        // Delete from meta cache
        try {
          metaCache.delete(f.toString());
        } catch (IOException e) {
          LOG.error("Failed to remove file from meta cache", e);
        }
      };
      unboundedThreadPool.submit(r);
    }
    return result;
  }

  public void close() throws IOException {
    this.remoteFS.close();
    if (this.writeCacheFS != null) {
      this.writeCacheFS.close();
    }
  }
  
  @VisibleForTesting
  static void dispose() {
    dataCache.dispose();
    cachedFS.clear();
  }

  private Path remoteToCachingPath(Path remotePath) {
    URI remoteURI = remotePath.toUri();
    String path = remoteURI.getPath();
    URI remoteFSURI = remoteFS.getUri();
    String remoteFSPath = remoteFSURI.getPath();
    String relativePath = path.substring(remoteFSPath.length());
    if (!relativePath.startsWith(File.separator)) {
      relativePath = File.separator + relativePath;
    }
    String cacheURIPath = writeCacheFS.getUri().toString();
    if (cacheURIPath.endsWith(File.separator)){
      cacheURIPath = cacheURIPath.substring(0, cacheURIPath.length() - 1); 
    }
    return new Path(cacheURIPath + relativePath);  
  }
  
  @SuppressWarnings("unused")
  private Path cachingToRemotePath(Path cachingPath) {
    URI cachingURI = cachingPath.toUri();
    String path = cachingURI.getPath();
    URI cachingFSURI = writeCacheFS.getUri();
    String cachePath = cachingFSURI.getPath();
    String relativePath = path.substring(cachePath.length());
    if (!relativePath.startsWith(File.separator)) {
      relativePath = File.separator + relativePath;
    }
    String remoteURIPath = remoteFS.getUri().toString();
    if (remoteURIPath.endsWith(File.separator)){
      remoteURIPath = remoteURIPath.substring(0, remoteURIPath.length() - 1); 
    }
    return new Path(remoteURIPath + relativePath);
  }

  private void checkEviction() {
    double storageOccupied = (double)this.writeCacheSize.get()/ this.writeCacheMaxSize;
    if (storageOccupied > 0.95) {
      Thread t = evictor.get();
      if (t != null && t.isAlive()) {
        return ; // eviction is in progress
      }
      // else
      t = new Thread(() -> {
        try {
          evictFiles();
        } catch (IOException e) {
          LOG.error("File evictor", e);
        }
      });
      boolean result = evictor.compareAndSet(null, t);
      if (!result) {
        return; // another thread started eviction
      }
      t.start();
    }
  }
  
  @Override
  public void bytesWritten(SidecarCachingOutputStream stream, long bytes) {
    writeCacheSize.addAndGet(bytes);
    checkEviction();
  }

  @Override
  public void closingRemote(SidecarCachingOutputStream stream) {
    long length = stream.length();
    Path path = stream.getRemotePath();
    // Add path - length to the FIFO cache
    Path cachePath = remoteToCachingPath(path);
    writeCacheFileList.put(cachePath.toString(), length);
    
    Runnable r = () -> {
      try {
        FSDataOutputStream os = stream.getRemoteStream();
        os.close();
        deleteMoniker(cachePath);
        metaCache.put(path.toString(), length, 0);
      } catch (IOException e) {
        //TODO - how to handle exception?
        LOG.error("Closing remote stream", e);
      }
    };
    unboundedThreadPool.submit(r);
  }

  @Override
  public void reportException(SidecarCachingOutputStream stream, Exception e) {
    LOG.warn("Sidecar caching output stream", e);
    FSDataOutputStream cacheOut = stream.getCachingStream();
    // Try to close caching stream then delete file
    Path cachePath = null;
    try {
      if (cacheOut != null) {
        cacheOut.close();
        Path remotePath = stream.getRemotePath();
        cachePath = remoteToCachingPath(remotePath);
        writeCacheFS.delete(cachePath, false);
      }
    } catch (IOException ee) {
      // swallow
      LOG.error("Failed to close and delete cache file {}", cachePath);
    }
    stream.disableCachingStream();
  }
  
  /**
   * Data cache eviction of pages procedure
   */
  
  private void evictDataPages(Path path) {
    Long size = null;
    try {
      size = (Long) metaCache.get(path.toString());
      if (size == null) {
        size = this.remoteFS.getFileStatus(path).getLen();
      }
    } catch (IOException e) {
      LOG.error("Evictor failed", e);
      return;
    }
    // Initialize base key
    String hash =  md5().hashString(path.toString(), UTF_8).toString();
    byte[] baseKey = new byte[hash.length() + Utils.SIZEOF_LONG];
    System.arraycopy(hash.getBytes(), 0, baseKey, 0, hash.length());
    
    long off = 0;
    while (off < size) {
      byte[] key = getKey(baseKey, off);
      try {
        dataCache.delete(key);
      } catch (IOException e) {
        LOG.error("Evictor failed", e);
        return;
      }
      off += dataPageSize;
    }
  }
  
  private byte[] getKey(byte[] baseKey, long offset) {
    int size = baseKey.length;
    offset = offset / dataPageSize * dataPageSize;
    for (int i = 0; i < Utils.SIZEOF_LONG; i++) {
      int rem = (int) (offset % 256);
      baseKey[size - i - 1] = (byte) rem;
      offset /= 256;
    }
    return baseKey;
  }
  
  /**
   * File eviction from write cache
   * @throws IOException 
   */
  
  private void evictFiles() throws IOException {
    if (!this.writeCacheEnabled) {
      return;
    }
    double usedStorageRatio = (double) this.writeCacheSize.get() / this.writeCacheMaxSize;
    // TODO: do we need to make this configurable?
    try {
      while (usedStorageRatio > 0.9) {
        String fileName = writeCacheFileList.evictionCandidate();
        
        LOG.debug("Evict file {}", fileName);
        
        if (fileName == null) {
          LOG.error("Write cache file list is empty");
          return;
        }
        long len = writeCacheFileList.get(fileName);
        
        this.writeCacheSize.addAndGet(-len);
        Path p = new Path(fileName);
        // Delete file in the local write cache
        try {
          this.writeCacheFS.delete(p, false);
          writeCacheFileList.remove(fileName);
        } catch (IOException e) {
          LOG.error("File evictor failed to delete file {}", fileName);
          throw e;
        }
        // Recalculate cache usage ratio
        usedStorageRatio = (double) this.writeCacheSize.get() / this.writeCacheMaxSize;
      }
    } finally {
      evictor.set(null);
    }
  }
}
