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

import static com.carrot.sidecar.util.SidecarConfig.DATA_CACHE_FILE_NAME;
import static com.carrot.sidecar.util.SidecarConfig.META_CACHE_NAME;
import static com.carrot.sidecar.util.Utils.hashCrypto;
import static com.carrot.sidecar.util.Utils.join;
import static com.carrot.sidecar.util.Utils.toBytes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrot.cache.Builder;
import com.carrot.cache.Cache;
import com.carrot.cache.controllers.LRCRecyclingSelector;
import com.carrot.cache.index.CompactBlockIndexFormat;
import com.carrot.cache.io.BlockDataWriter;
import com.carrot.cache.io.BlockFileDataReader;
import com.carrot.cache.io.BlockMemoryDataReader;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;
import com.carrot.sidecar.util.CacheType;
import com.carrot.sidecar.util.LRUCache;
import com.carrot.sidecar.util.SidecarConfig;


public class SidecarCachingFileSystem implements SidecarCachingOutputStream.Listener{
  
  private static final Logger LOG = LoggerFactory.getLogger(SidecarCachingFileSystem.class);  
  /*
   *  Local cache for remote file data
   */
  private static Cache dataCache; // singleton
  
  /**
   * Data cache type (offheap, file, hybrid)
   */
  
  private static CacheType dataCacheType; 
  /*
   *  Caches remote file lengths by file path
   */
  private static Cache metaCache; // singleton

  /*
   *  LRU cache for cached on write filenames with their lengths (if enabled) 
   */
  private static LRUCache<String, Long> writeCacheFileList;
  
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
   *  Is cache-on-write enabled - if yes, remote FS output stream close()
   *  can be async
   */
  private volatile boolean writeCacheEnabled;
  
  
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

  private void setDataCacheType(CarrotConfig config, CacheType type) {
    if (type == CacheType.HYBRID) {
      addCacheType(config, CacheType.OFFHEAP.getCacheName(), CacheType.OFFHEAP.getType());
      addCacheType(config, CacheType.FILE.getCacheName(), CacheType.FILE.getType());
    } else {
      addCacheType(config, type.getCacheName(), type.getType());
    }
  }
  
  /**
   * Add single cache type (memory or disk)
   * @param confug
   * @param type
   */
  private void addCacheType(CarrotConfig config, String cacheName, String type) {
    String[] names = config.getCacheNames();
    String[] types = config.getCacheTypes();
    
    String[] newNames = new String[names.length + 1];
    System.arraycopy(names, 0, newNames, 0, names.length);
    newNames[newNames.length - 1] = cacheName;
    String[] newTypes = new String[types.length + 1];
    System.arraycopy(types, 0, newTypes, 0, types.length);
    newTypes[newTypes.length - 1] = type;
    String cacheNames = join(newNames, ",");
    String cacheTypes = join(newTypes, ",");
    config.setCacheNames(cacheNames);
    config.setCacheTypes(cacheTypes);
  }
  
  /**
   * Used for testing
   * @param b
   */
  void setMetaCacheEnabled(boolean b) {
    this.metaCacheable = b;
  }
  
  /**
   * Is meta cache enabled
   * @return true or false
   */
  boolean isMetaCacheEnabled() {
    return this.metaCacheable;
  }
  
  /**
   * Set enable/disable write cache (Eviction thread can temporarily disable write cache)
   * @param b true or false
   */
  void setWriteCacheEnabled(boolean b) {
    if (this.writeCacheFS == null) {
      return;
    }
    this.writeCacheEnabled = b;
  }
  
  /**
   * Is write cache enabled
   * @return 
   */
  boolean isWriteCacheEnabled() {
    return this.writeCacheEnabled;
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
      
      final SidecarConfig sconfig = SidecarConfig.fromHadoopConfiguration(configuration);
      dataCacheType = sconfig.getDataCacheType();
      // Add two caches (sidecar-data, sidecar-meta) types to the configuration
      setDataCacheType(config, dataCacheType);      
      // meta cache is always offheap
      addCacheType(config, META_CACHE_NAME, "offheap");
      
      this.dataPageSize = (int) sconfig.getDataPageSize();
      this.ioBufferSize = (int) sconfig.getIOBufferSize();
      this.ioPoolSize = (int) sconfig.getIOPoolSize();
      this.writeCacheEnabled = sconfig.isWriteCacheEnabled();
      if (this.writeCacheEnabled) {
        writeCacheMaxSize = sconfig.getWriteCacheSizePerInstance();
        URI writeCacheURI = sconfig.getWriteCacheURI();
        if (writeCacheURI != null) {
          if (sconfig.isTestMode()) {
            //TODO: we should not have this test mode at all
            this.writeCacheFS = new LocalFileSystem();
            this.writeCacheFS.initialize(writeCacheURI, configuration);
          } else {
            this.writeCacheFS = FileSystem.get(writeCacheURI, configuration);
          }
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
        loadWriteCacheFileListCache();

        if (!sconfig.isTestMode()) {
          // Install shutdown hook if not in a test mode
          Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
              if (sconfig.isCachePersistent()) {
                saveDataCache();
                saveMetaCache();
                LOG.info("Shutdown hook installed for cache[{}]", dataCache.getName());
                LOG.info("Shutdown hook installed for cache[{}]", metaCache.getName());
              }
              // we save write cache file list even if persistence == false
              saveWriteCacheFileListCache();
              LOG.info("Shutdown hook installed for cache[lru-cache]");
            } catch (IOException e) {
              LOG.error(e.getMessage(), e);
            }
          }));
        }
      }
      this.inited = true;
      //TODO: make it configurable
      int coreThreads = sconfig.getSidecarThreadPoolMaxSize();
      int keepAliveTime = 60; // hard-coded
      //TODO: should it be bounded or unbounded?
      // This is actually unbounded queue (LinkedBlockingQueue w/o parameters)
      // and bounded thread pool - only coreThreads maximum
      unboundedThreadPool = new ThreadPoolExecutor(
        coreThreads, Integer.MAX_VALUE, 
        keepAliveTime, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        BlockingThreadPoolExecutorService.newDaemonThreadFactory(
            "sidecar-thread-pool"));
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e);
    }
 

  }

  /**
   * For testing only (not visible outside the package)
   */
  
  boolean deleteFromWriteCache(Path remotePath) throws IOException
  {
    if (this.writeCacheFS == null) {
      throw new IOException("Write caching FS is not available");
    }
    Path cachedPath = remoteToCachingPath(remotePath);
    return this.writeCacheFS.delete(cachedPath, false);
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
  
  private void loadWriteCacheFileListCache() throws IOException {
    CarrotConfig config = CarrotConfig.getInstance();
    String snapshotDir = config.getSnapshotDir(LRUCache.NAME);
    String fileName = snapshotDir + File.separator + LRUCache.FILE_NAME;
    File file = new File(fileName);
    writeCacheFileList = new LRUCache<>();

    if (file.exists()) {
      FileInputStream fis = new FileInputStream(file);
      DataInputStream dis = new DataInputStream(fis);
      writeCacheSize.set(dis.readLong());
      writeCacheFileList.load(dis);
      dis.close();
      LOG.info("Loaded cache[{}]", LRUCache.NAME);
    } else {
      LOG.info("Created new cache[{}]", LRUCache.NAME);
    }
  }

  private void loadMetaCache() throws IOException{
    CarrotConfig config = CarrotConfig.getInstance();
    SidecarConfig sconfig = SidecarConfig.getInstance();
    try {
      String rootDir = config.getGlobalCacheRootDir();
      long maxSize = config.getCacheMaximumSize(META_CACHE_NAME);
      int dataSegmentSize = (int) config.getCacheSegmentSize(META_CACHE_NAME);
      metaCache = Cache.loadCache(rootDir, META_CACHE_NAME);
      if (metaCache == null) {
        Builder builder = new Builder(META_CACHE_NAME);
        builder = builder
            
            .withCacheMaximumSize(maxSize)
            .withCacheDataSegmentSize(dataSegmentSize)
            .withRecyclingSelector(LRCRecyclingSelector.class.getName())
            .withDataWriter(BlockDataWriter.class.getName())
            .withMemoryDataReader(BlockMemoryDataReader.class.getName())
            .withFileDataReader(BlockFileDataReader.class.getName())
            .withMainQueueIndexFormat(CompactBlockIndexFormat.class.getName());       
        metaCache = builder.buildMemoryCache();
        LOG.info("Created new cache[{}]", metaCache.getName());
      } else {
        LOG.info("Loaded cache[{}] from the path: {}", metaCache.getName(),
          config.getCacheRootDir(metaCache.getName()));      }
    } catch (IOException e) {
      LOG.error("loadMetaCache error", e);
      throw e;
    }     
    if (sconfig.isJMXMetricsEnabled()) {
      LOG.info("SidecarCachingFileSystem JMX enabled for meta-data cache");
      String domainName = config.getJMXMetricsDomainName();
      metaCache.registerJMXMetricsSink(domainName);
    }  
  }

  private Cache loadDataCache(CacheType type) throws IOException {
    CarrotConfig config = CarrotConfig.getInstance();
    boolean isPersistent = SidecarConfig.getInstance().isCachePersistent();
    Cache dataCache = null;
    if (isPersistent) {
      try {
        dataCache = Cache.loadCache(type.getCacheName());
        if (dataCache != null) {
          LOG.info("Loaded cache[{}] from the path: {}", dataCache.getName(),
            config.getCacheRootDir(dataCache.getName()));
          return dataCache;
        }
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }
    }

    if (dataCache == null) {
      // Create new instance
      dataCache = new Cache(type.getCacheName(), config);
      LOG.info("Created new cache[{}]", dataCache.getName());
    }
    SidecarConfig sconfig = SidecarConfig.getInstance();
    boolean metricsEnabled = sconfig.isJMXMetricsEnabled();

    if (metricsEnabled) {
      String domainName = sconfig.getJMXMetricsDomainName();
      LOG.info("SidecarCachingFileSystem JMX enabled for data cache");
      dataCache.registerJMXMetricsSink(domainName);
    }
    return dataCache;
  }
  
  private Cache loadDataCache() throws IOException {
    if (dataCacheType != CacheType.HYBRID) {
      dataCache = loadDataCache(dataCacheType);
      return dataCache;
    } else {
      dataCache = loadDataCache(CacheType.OFFHEAP);
      Cache victimCache = loadDataCache(CacheType.FILE);
      dataCache.setVictimCache(victimCache);
      return dataCache;
    }
  }
  
  void saveWriteCacheFileListCache() throws IOException {
    if (writeCacheFileList != null) {
      long start = System.currentTimeMillis();
      LOG.info("Shutting down cache[{}]", LRUCache.NAME);
      CarrotConfig config = CarrotConfig.getInstance();
      String snapshotDir = config.getSnapshotDir(LRUCache.NAME);
      FileOutputStream fos = new FileOutputStream(snapshotDir + 
      File.separator + LRUCache.FILE_NAME);
      // Save total write cache size
      DataOutputStream dos = new DataOutputStream(fos);
      dos.writeLong(writeCacheSize.get());
      writeCacheFileList.save(dos);
      // do not close - it was closed already
      long end = System.currentTimeMillis();
      LOG.info("Shutting down cache[{}] done in {}ms",LRUCache.NAME , (end - start));
    }
  }

  void saveDataCache() throws IOException {
    long start = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}]", DATA_CACHE_FILE_NAME);
    dataCache.shutdown();
    long end = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}] done in {}ms", DATA_CACHE_FILE_NAME, (end - start));
  }
  
  void saveMetaCache() throws IOException {
    long start = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}]", META_CACHE_NAME);
    metaCache.shutdown();
    long end = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}] done in {}ms", META_CACHE_NAME, (end - start));
  }
  
  public static void dispose() {
    dataCache.dispose();
    metaCache.dispose();
    cachedFS.clear();
    dataCache = null;
    metaCache = null;
    writeCacheSize.set(0);
  }

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

  /**
   * Single thread only, but I think we are OK?
   */
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
  
  /*********************************
   * 
   * Meta data cache access start
   *********************************/
  /**
   * Check if meta exists in the cache for a particular
   * file path 
   * @param p file path
   * @return true - yes, false - otherwise
   */
  private boolean metaExists(Path p) {
    byte[] hashedKey = hashCrypto(p.toString().getBytes());
    boolean b = metaExists(hashedKey);
    return b;
  }
  
  private boolean metaExists(byte[] key) {
    if (!metaCacheable) {
      return false;
    }
    return metaCache.exists(key);
  }
  /**
   * Put meta (currently, only file length later will add modification time)
   * @param p file path
   * @param length file length
   * @return true on success, false - otherwise
   */
  private boolean metaPut(Path p, long length) {
    byte[] hashedKey = hashCrypto(p.toString().getBytes());
    return metaPut(hashedKey, length);
  }
  
  /**
   * TODO: do not ignore failed put - check operation result
   * @param key hashed key for a path
   * @param length length of a file
   * @return true or false
   */
  private boolean metaPut(byte[] key, long length) {
    if (!metaCacheable) {
      return false;
    }
    try {
      return metaCache.put(key, toBytes(length), 0);
    } catch (IOException e) {
      //TODO: proper error handling
      LOG.error("Can not save file meta", e);
    }
    return false;
  }
  
  private long metaGet (Path p) {
    byte[] hashedKey = hashCrypto(p.toString().getBytes());
    return metaGet(hashedKey);
  }

  /**
   * Length of a file or -1
   * @param key file key (hashed path value to 16 bytes crypto MD5)
   * @return
   */
  private long metaGet(byte[] key) {
    if (!metaCacheable) {
      return -1;
    }
    // 32 is sufficient to keep the whole key (16) + value (8) 
    byte[] buf = new byte[32];
    try {
      int size = (int) metaCache.get(key,  0,  key.length, buf, 0);
      if (size < 0) {
        // does not exist in the meta cache
        return -1;
      }
      return UnsafeAccess.toLong(buf, 0);
      
    } catch (IOException e) {
    //TODO: proper error handling
      LOG.error("Can not get file meta", e);
    }
    return -1;
  }
  
  private boolean metaDelete(Path p) {
    byte[] hashedKey = hashCrypto(p.toString().getBytes());
    return metaDelete(hashedKey);
  }
  
  private boolean metaDelete(byte[] hashedKey) {
    if (!metaCacheable) {
      return false;
    }
    try {
      return metaCache.delete(hashedKey);
    } catch (IOException e) {
      //TODO: prper exception handling
      LOG.error("Can not delete file meta", e);
    }
    return false;
  }

  /**
   * Saves meta, checks if it exists first
   * TODO: requires putIfAbsent API support
   * @param path path to a file
   * @param length length of a file
   */
  private void metaSave(Path path, long length) {
    if (metaCacheable) {
      // TODO: remove this comment after debug
      // byte[] key = path.toString().getBytes();
      if (!metaExists(path)) {
        boolean result = metaPut(path, length);
        if (!result) {
          LOG.error("Failed to save meat for {}", path);
        }
      }
    }
  }
  /*********************************
   * 
   * Meta data cache access end
   *********************************/
  
  /*********************************
   * 
   * Data cache access start - dataCache is used in SCFS 
   * only to delete file data pages
   * TODO: in the future this call must be delegated
   * to DataCache provider, which must optimize
   * the call and distribute it to all affected
   * caching servers
   *********************************/
  private boolean dataDeleteFile(Path path, long size) 
  {
    LOG.debug("Evict data pages for {} length={}", path, size);

    byte[] baseKey = getBaseKey(path);
    long off = 0;
    while (off < size) {
      byte[] key = getKey(baseKey, off);
      try {
        boolean res = dataCache.delete(key);
        LOG.debug("Delete {} result={}", off,  res);
      } catch (IOException e) {
        LOG.error("Evictor failed", e);
        return false;
      }
      off += dataPageSize;
    }
    return true;
  }
  
  /*********************************
   * 
   * Data cache access end
   *********************************/
  
  @Override
  public void bytesWritten(SidecarCachingOutputStream stream, long bytes) {
    if (isWriteCacheEnabled()) {
      writeCacheSize.addAndGet(bytes);
      checkEviction();
    }
  }

  @Override
  public void closingRemote(SidecarCachingOutputStream stream) {
    long length = stream.length();
    final Path path = stream.getRemotePath();

    if (writeCacheEnabled) {
      // It is save to update meta first in the cache
      // because write cache has already file ready to read
      metaSave(path, length);
      // ASYNC
      Path cachePath = remoteToCachingPath(path);
      writeCacheFileList.put(cachePath.toString(), length);
      Runnable r = () -> {
        try {
          FSDataOutputStream os = stream.getRemoteStream();
          os.close();
          if (writeCacheEnabled) {
            deleteMoniker(remoteToCachingPath(path));
          }
        } catch (IOException e) {
          // TODO - how to handle exception?
          LOG.error("Closing remote stream", e);
        }
      };
      unboundedThreadPool.submit(r);
    } else {
      //SYNC
      try {
        FSDataOutputStream os = stream.getRemoteStream();
        // This is where all actual data transmission start
        os.close();
        // now update meat
        metaSave(path, length);
      } catch (IOException e) {
        // TODO - how to handle exception?
        LOG.error("Closing remote stream", e);
      }
    }
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
        if (this.writeCacheEnabled) {
          cachePath = remoteToCachingPath(remotePath);
          writeCacheFS.delete(cachePath, false);
        }
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
  
  private void evictDataPages(Path path, long len) {
    if (len < 0) {
      LOG.debug("evictDataPages: file {} path is not in the meta cache", path);
      return;
    }
    dataDeleteFile(path, len);
  }
  
  private byte[] getBaseKey(Path path)  {
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
    }
    md.update(path.toString().getBytes());
    byte[] digest = md.digest();
    byte[] baseKey = new byte[digest.length + Utils.SIZEOF_LONG];
    System.arraycopy(digest, 0, baseKey, 0, digest.length);
    return baseKey;
  }
  
  private long getFileLength(Path p) throws IOException {
    long len = metaGet(p);
    if (len > 0) {
      return len;
    }
    // This can throw FileNotFound exception
    FileStatus fs = remoteFS.getFileStatus(p);
    len = fs.getLen();
    metaSave(p, len);
    return len;
  }
  
  private boolean isFile(Path p) throws IOException {
    boolean result;
    if (metaExists(p)) {
      // we keep only files in meta
      return true;
    }
    FileStatus fs = remoteFS.getFileStatus(p);
    result = fs.isFile();
    if (result && metaCacheable) {
      metaSave(p, fs.getLen());
    }
    return result;
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
    LOG.info("Write cache file number={} write cache size={} writeCacheMaxSize={}",
      writeCacheFileList.size(), writeCacheSize.get(), writeCacheMaxSize );
    try {
      while (usedStorageRatio > writeCacheEvictionStopsAt) {
        String fileName = writeCacheFileList.evictionCandidate();
        if (fileName == null) {
          LOG.error("Write cache file list is empty");
          return;
        }
        LOG.info("Evict file {}", fileName);
        Long len = writeCacheFileList.get(fileName);
        Path p = new Path(fileName);
        boolean exists = this.writeCacheFS.exists(p);
        Path moniker = getFileMonikerPath(p);
        if (this.writeCacheFS.exists(moniker)) {
          // Disable write cache temporarily
          setWriteCacheEnabled(false);
          // Log warning
          LOG.warn("Disable write cache, eviction candidate {} has not been synced to remote yet", p);
          // wait 1 minute
          try {
            Thread.sleep(60000);
          } catch (InterruptedException e) {
          } 
          continue;
        }
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
      if (isWriteCacheEnabled() == false) {
        // Enable write cache if it was disabled
        setWriteCacheEnabled(true);
      }
    }
  }
  
  public static Cache getDataCache() {
    return dataCache;
  }
  
  public static Cache getMetaCache() {
    return metaCache;
  }
  
  public static LRUCache<String,Long> getWriteCacheFileListCache() {
    return writeCacheFileList;
  }
  
  public static void clearFSCache() {
    cachedFS.clear();
  }
  
  public void shutdown() throws IOException {
    boolean isPersistent = SidecarConfig.getInstance().isCachePersistent();
    if (isPersistent) {
      saveDataCache();
      saveMetaCache();
    }
    saveWriteCacheFileListCache();
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
      return ((CachingFileSystem)remoteFS).openRemote(path, bufferSize);
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
  
    LOG.debug("Create file: {}", f);
    FSDataOutputStream remoteOut = 
        ((CachingFileSystem)remoteFS).createRemote(f, permission, overwrite, bufferSize, replication, blockSize, progress);
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
  
  public boolean mkdirs(Path path, FsPermission permission)
      throws IOException, FileAlreadyExistsException {
    LOG.debug("Create dir: {}", path);
    boolean result = ((CachingFileSystem)remoteFS).mkdirsRemote(path, permission);
    if (result && this.writeCacheFS != null) {
      this.writeCacheFS.mkdirs(path); // we use default permission
    }
    return result;
  }
  
  private void createMoniker(Path cachePath) throws IOException {
    Path p = getFileMonikerPath(cachePath);
    FSDataOutputStream os = this.writeCacheFS.create(p);
    os.close();
  }

  private void deleteMoniker(Path cachePath) throws IOException {
    Path p = getFileMonikerPath(cachePath);
    this.writeCacheFS.delete(p, false);
  }
  
  private Path getFileMonikerPath(Path file) {
    return new Path(file.toString() + ".toupload");
  }
  
  @SuppressWarnings("deprecation")
  public FSDataOutputStream createNonRecursive(Path path, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    
    LOG.debug("Create non-recursive {}", path);
    FSDataOutputStream remoteOut = 
        ((CachingFileSystem)remoteFS).createNonRecursiveRemote(path, permission, flags, bufferSize, 
          replication, blockSize, progress);

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
    FSDataOutputStream remoteOut = ((CachingFileSystem)remoteFS).appendRemote(f, bufferSize, progress);

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
    LOG.info("Rename {}\n to {}", src, dst);
    // Check if src is file
    boolean isFile = isFile(src);
    boolean result = ((CachingFileSystem) remoteFS).renameRemote(src, dst);
    if (result && isFile) {
      long len = metaGet(src);
      if (len > 0) {
        metaDelete(src);
        metaPut(dst, len);
      }
      // Clear data pages cache
      Runnable r = () -> {
        evictDataPages(src, len);
        // Update meta cache
      };
      unboundedThreadPool.submit(r);
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
        LOG.error(String.format("Failed to rename {} to {}", cacheSrc, cacheDst), e);
      }
    }
    return result;
  }

  public boolean delete(Path f, boolean recursive) throws IOException {

    LOG.debug("Delete {} recursive={}", f, recursive);
    boolean isFile = metaExists(f);
    boolean result = ((CachingFileSystem)remoteFS).deleteRemote(f, recursive);
   
    // Check if f is file
    if (isFile && result) {
      // Clear data pages cache
      final long len = metaGet(f);
      Runnable r = () -> {
        evictDataPages(f, len);
        metaDelete(f);
      };
      unboundedThreadPool.submit(r);
    } 

    if (this.writeCacheEnabled) {
      Path p = remoteToCachingPath(f);
      try {
        // Delete from cache file list
        writeCacheFileList.remove(p.toString());
        this.writeCacheFS.delete(p, recursive);
      } catch (IOException e) {
        LOG.error("Delete write-cache-fs path={} failed", f, e);
      }
    }
    return result;
  }

  public void close() throws IOException {
    //TODO: what to do on close?
    if (this.writeCacheFS != null) {
      this.writeCacheFS.close();
    }
  }
}
