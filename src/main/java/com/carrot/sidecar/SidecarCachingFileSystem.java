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

import java.io.DataInputStream;
import java.io.DataOutputStream;
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
import com.carrot.sidecar.util.SidecarConfig;
import com.google.common.annotations.VisibleForTesting;

public class SidecarCachingFileSystem implements SidecarCachingOutputStream.Listener{
  
  private static final Logger LOG = LoggerFactory.getLogger(SidecarCachingFileSystem.class);

  private final static String DATA_CACHE_NAME = SidecarConfig.DATA_CACHE_NAME;
  private final static String META_CACHE_NAME = SidecarConfig.META_CACHE_NAME;
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
  
  /*
   * Caching {FS.URI, sidecar instance}  
   */
  private static ConcurrentHashMap<String, SidecarCachingFileSystem> cachedFS =
      new ConcurrentHashMap<>();

  /*
   * When file eviction starts from write cache
   */
  private static double writeCacheEvictionStartsAt = 0.95;
  
  /*
   * When file eviction stops in write cache
   */
  private static double writeCacheEvictionStopsAt = 0.9;

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
  
  
  /**
   * Is meta data cacheable
   */
  private boolean metaCacheable;
  
  /*
   *  Write cache maximum size (per server instance)
   */
  static long writeCacheMaxSize;
  
  /*
   *  Current write cache size 
   */
  static AtomicLong writeCacheSize = new AtomicLong();
  
  /*
   * File eviction thread (from cache-on-write cache)
   */
  static AtomicReference<Thread> evictor = new AtomicReference<>();

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
    this.metaCacheable = fs instanceof MetaDataCacheable;
  }

  private void setCacheTypes(CarrotConfig config) {
    String[] names = config.getCacheNames();
    String[] types = config.getCacheTypes();
    
    String[] newNames = new String[names.length + 2];
    System.arraycopy(names, 0, newNames, 0, names.length);
    
    newNames[newNames.length - 2] = SidecarConfig.DATA_CACHE_NAME;
    newNames[newNames.length - 1] = SidecarConfig.META_CACHE_NAME;
    
    String[] newTypes = new String[types.length + 2];
    System.arraycopy(types, 0, newTypes, 0, types.length);
    newTypes[newTypes.length - 2] = "file";
    newTypes[newTypes.length - 1] = "offheap";
    
    String cacheNames = com.carrot.sidecar.util.Utils.join(newNames, ",");
    String cacheTypes = com.carrot.sidecar.util.Utils.join(newTypes, ",");
    config.setCacheNames(cacheNames);
    config.setCacheTypes(cacheTypes);

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
      // Add two caches (sidecar-data,sidecar-meta) types to the configuration
      setCacheTypes(config);      
      SidecarConfig sconfig = SidecarConfig.fromHadoopConfiguration(configuration);
      
      this.dataPageSize = (int) sconfig.getDataPageSize();
      this.ioBufferSize = (int) sconfig.getIOBufferSize();
      this.ioPoolSize = (int) sconfig.getIOPoolSize();
      this.writeCacheEnabled = sconfig.isWriteCacheEnabled();
      if (this.writeCacheEnabled) {
        writeCacheMaxSize = sconfig.getWriteCacheSizePerInstance();
        URI writeCacheURI = sconfig.getWriteCacheURI();
        if (writeCacheURI != null) {
          this.writeCacheFS = FileSystem.get(writeCacheURI, configuration);
          // Set working directory for cache
          this.writeCacheFS.setWorkingDirectory(new Path(writeCacheURI.toString()));
        } else {
          this.writeCacheEnabled = false;
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
        
        if (!sconfig.isTestEnabled()) {
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
          LOG.info("Shutdown hook installed for cache[{}]", metaCache.getName());
          LOG.info("Shutdown hook installed for cache[fifo-cache]");

        }
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

  /**
   * Get write cache FS
   * @return write cache file system
   */
  public FileSystem getWriteCacheFS() {
    return this.writeCacheFS;
  }
  
  /**
   * Get remote FS
   * @return remote file system
   */
  public FileSystem getRemoteFS (){
    return this.remoteFS;
  }
  
  private void loadFIFOCache() throws IOException {
    CarrotConfig config = CarrotConfig.getInstance();
    String snapshotDir = config.getSnapshotDir(FIFOCache.NAME);
    String fileName = snapshotDir + File.separator + FIFOCache.FILE_NAME;
    File file = new File(fileName);
    writeCacheFileList = new FIFOCache<>();

    if (file.exists()) {
      FileInputStream fis = new FileInputStream(file);
      DataInputStream dis = new DataInputStream(fis);
      writeCacheSize.set(dis.readLong());
      writeCacheFileList.load(dis);
      dis.close();
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
  
  void saveFIFOCache() throws IOException {
    if (writeCacheFileList != null) {
      long start = System.currentTimeMillis();
      LOG.info("Shutting down cache[{}]", FIFOCache.NAME);
      CarrotConfig config = CarrotConfig.getInstance();
      String snapshotDir = config.getSnapshotDir(FIFOCache.NAME);
      FileOutputStream fos = new FileOutputStream(snapshotDir + 
      File.separator + FIFOCache.FILE_NAME);
      // Save total write cache size
      DataOutputStream dos = new DataOutputStream(fos);
      dos.writeLong(writeCacheSize.get());
      writeCacheFileList.save(dos);
      // do not close - it was closed already
      long end = System.currentTimeMillis();
      LOG.info("Shutting down cache[{}] done in {}ms",FIFOCache.NAME , (end - start));
    }
  }

  void saveDataCache() throws IOException {
    long start = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}]", DATA_CACHE_NAME);
    dataCache.shutdown();
    long end = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}] done in {}ms", DATA_CACHE_NAME, (end - start));
  }
  
  void saveMetaCache() throws IOException {
    long start = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}]", META_CACHE_NAME);
    metaCache.shutdown();
    long end = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}] done in {}ms", META_CACHE_NAME, (end - start));
  }
  
  @VisibleForTesting
  public static void dispose() {
    dataCache.dispose();
    metaCache.getNativeCache().dispose();
    cachedFS.clear();
    dataCache = null;
    metaCache = null;
  }

  @VisibleForTesting
  Path remoteToCachingPath(Path remotePath) {
    URI remoteURI = remotePath.toUri();
    String path = remoteURI.getPath();
    String relativePath = path; 
    if (!relativePath.startsWith(File.separator)) {
      relativePath = File.separator + relativePath;
    }
    Path cacheWorkDir = writeCacheFS.getWorkingDirectory();
    String fullPath = cacheWorkDir + relativePath;
    Path cachePath =  new Path(fullPath);  

    return writeCacheFS.makeQualified(cachePath);
  }
  
  // Not used
  @VisibleForTesting
  Path cachingToRemotePath(Path cachingPath) {
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
    double storageOccupied = (double) writeCacheSize.get() / writeCacheMaxSize;
    if (storageOccupied > writeCacheEvictionStartsAt) {
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
        if (metaCacheable) {
          metaCache.put(path.toString(), length, 0);
        }
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
    LOG.debug("Evict data pages for {}", path);

    try {
      size = getFileLength(path);
    } catch (IOException e) {
      LOG.error("Evictor failed for path {}", path);
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
        boolean res = dataCache.delete(key);
        LOG.debug("Delete {} result={}", off,  res);
      } catch (IOException e) {
        LOG.error("Evictor failed", e);
        return;
      }
      off += dataPageSize;
    }
  }
  
  private long getFileLength(Path p) throws IOException {
    if (metaCacheable) {
      Long len = (Long) metaCache.get(p.toString());
      if (len != null) {
        return len;
      }
    }
    // This can throw FileNotFound exception
    FileStatus fs = remoteFS.getFileStatus(p);
    long len = fs.getLen();
    if (metaCacheable) {
      metaCache.put(p.toString(), len, 0);
    }
    return fs.getLen();
  }
  
  private boolean isFile(Path p) throws IOException {
    if (metaCacheable) {
      return metaCache.exists(p.toString());
    }
    FileStatus fs = remoteFS.getFileStatus(p);
    return fs.isFile();
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

    double usedStorageRatio = (double) writeCacheSize.get() / writeCacheMaxSize;
    
    try {
      while (usedStorageRatio > writeCacheEvictionStopsAt) {
        String fileName = writeCacheFileList.evictionCandidate();
        
        LOG.debug("Evict file {}", fileName);
        if (fileName == null) {
          LOG.error("Write cache file list is empty");
          return;
        }
        Long len = writeCacheFileList.get(fileName);
        Path p = new Path(fileName);
        boolean exists = this.writeCacheFS.exists(p);
        if (len == null && exists) {
          len = this.writeCacheFS.getFileStatus(p).getLen();
        } else if (!exists){
          writeCacheFileList.remove(fileName);
          continue;
        }
        // Delete file in the local write cache
        try {
          boolean res = this.writeCacheFS.delete(p, false);
          if (res) {
            writeCacheFileList.remove(fileName);
            writeCacheSize.addAndGet(-len);
          }
        } catch (IOException e) {
          LOG.error("File evictor failed to delete file {}", fileName);
          throw e;
        }
        // Recalculate cache usage ratio
        usedStorageRatio = (double) writeCacheSize.get() / writeCacheMaxSize;
      }
    } finally {
      evictor.set(null);
    }
  }
  
  @VisibleForTesting
  public static Cache getDataCache() {
    return dataCache;
  }
  
  @VisibleForTesting
  public static ObjectCache getMetaCache() {
    return metaCache;
  }
  
  @VisibleForTesting
  public static FIFOCache<String,Long> getFIFOCache() {
    return writeCacheFileList;
  }
  
  @VisibleForTesting
  public static void clearFSCache() {
    cachedFS.clear();
  }
  
  @VisibleForTesting
  public void shutdown() throws IOException {
    saveDataCache();
    saveMetaCache();
    saveFIFOCache();
    dispose();
    clearFSCache();
  }
  
  /**********************************************************************************
   * 
   * Hadoop FileSystem API
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
        
    Callable<FSDataInputStream> remoteCall = () -> {
      return remoteFS.open(path, bufferSize);
    };
    Path writeCachePath = remoteToCachingPath(path);
    Callable<FSDataInputStream> cacheCall = () -> {
      return writeCacheFS == null? null: writeCacheFS.open(writeCachePath, bufferSize);
    };
    
    long fileLength = getFileLength(path);
    FSDataInputStream cachingInputStream =
        new FSDataInputStream(new SidecarCachingInputStream(dataCache, path, remoteCall, cacheCall, fileLength,
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
        cacheOut = this.writeCacheFS.create(cachePath, overwrite, bufferSize, replication, blockSize);
    } catch (IOException e) {
      LOG.error("Write cache create file failed", e);
      return remoteOut;
    }

    // Create file moniker
    createMoniker(cachePath);
    return new FSDataOutputStream(new SidecarCachingOutputStream(cacheOut, remoteOut, f, this));
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
    return new FSDataOutputStream(new SidecarCachingOutputStream(cacheOut, remoteOut, path, this));
  }

  @SuppressWarnings("deprecation")
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    // Object storage FS do not support this operation, at least S3
    LOG.debug("Append {}", f);
    FSDataOutputStream remoteOut = this.remoteFS.append(f, bufferSize, progress);

    // Usually append is not supported by cloud object stores
    if (!this.writeCacheEnabled) {
      return remoteOut;
    }
    Path cachePath = remoteToCachingPath(f);
    
    FSDataOutputStream cacheOut = null;
    try {
      cacheOut = this.writeCacheFS.append(cachePath, bufferSize);
    } catch(Exception e) {
      // File does not exists or some other I/O issue
      return remoteOut;
    }
    return new FSDataOutputStream(new SidecarCachingOutputStream(cacheOut, remoteOut, f, this));
  }

  public boolean rename(Path src, Path dst) throws IOException {
    LOG.debug("Rename {} to {}", src, dst);
    // Check if src is file
    boolean isFile = isFile(src);
    boolean result = this.remoteFS.rename(src, dst);
    if (result && isFile) {
      // Clear data pages cache
      Runnable r = () -> {
        evictDataPages(src);
        // Update meta cache
        try {
          // TODO: is it correct?
          if (metaCacheable) {
            String key = src.toString();
            Long len = (Long) metaCache.get(key);
            if (len != null) {
              metaCache.delete(key);
              metaCache.put(dst.toString(), len, 0);
            }
          }
        } catch (IOException e) {
          LOG.error("rename", e);
        }
      };
      unboundedThreadPool.submit(r);
    }
    if (!this.writeCacheEnabled) {
      return result;
    }

    Path cacheSrc = null, cacheDst = null;
    try {
      cacheSrc = remoteToCachingPath(src);
      cacheDst = remoteToCachingPath(dst);
      if (this.writeCacheFS.exists(cacheSrc)) {
        this.writeCacheFS.rename(cacheSrc, cacheDst);
        // Remove from write-cache file list
        writeCacheFileList.remove(cacheSrc.toString());
      }
    } catch (IOException e) {
      LOG.error(String.format("Failed to rename %s to %s", cacheSrc, cacheDst), e);
    }
    return result;
  }

  public boolean delete(Path f, boolean recursive) throws IOException {

    LOG.debug("Delete {} recursive={}", f, recursive);

    boolean isFile = metaCache.exists(f.toString());
    boolean result = this.remoteFS.delete(f, recursive);
   
    // Check if f is file
    if (isFile && result) {
      // Clear data pages cache
      Runnable r = () -> {
        evictDataPages(f);
        // Delete from meta
        try {
          if (metaCacheable) {
            metaCache.delete(f.toString());
          }
        } catch (IOException e) {
          //TODO: this thing is serious
          LOG.error("Failed to delete key from meta-cache", e);
        }
      };
      unboundedThreadPool.submit(r);
    } else {
      if (metaCacheable) {
        metaCache.delete(f.toString());
      }
    }

    if (this.writeCacheEnabled) {
      Path p = remoteToCachingPath(f);
      try {
        this.writeCacheFS.delete(p, recursive);
        // Delete from cache file list
        writeCacheFileList.remove(p.toString());
      } catch (IOException e) {
        LOG.error("Delete write-cache-fs path={} failed", f, e);
      }
    }
    return result;
  }

  public void close() throws IOException {
    this.remoteFS.close();
    if (this.writeCacheFS != null) {
      this.writeCacheFS.close();
    }
  }
  
  
}
