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

import static com.carrotdata.sidecar.SidecarConfig.DATA_CACHE_FILE_NAME;
import static com.carrotdata.sidecar.SidecarConfig.META_CACHE_NAME;
import static com.carrotdata.sidecar.util.Utils.getBaseKey;
import static com.carrotdata.sidecar.util.Utils.getKey;
import static com.carrotdata.sidecar.util.Utils.hashCrypto;
import static com.carrotdata.sidecar.util.Utils.join;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.Builder;
import com.carrotdata.cache.Cache;
import com.carrotdata.cache.controllers.LRCRecyclingSelector;
import com.carrotdata.cache.index.CompactBlockIndexFormat;
import com.carrotdata.cache.io.BlockDataWriter;
import com.carrotdata.cache.io.BlockFileDataReader;
import com.carrotdata.cache.io.BlockMemoryDataReader;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;
import com.carrotdata.sidecar.hints.CachingHintDetector;
import com.carrotdata.sidecar.jmx.SidecarJMXSink;
import com.carrotdata.sidecar.jmx.SidecarSiteJMXSink;
import com.carrotdata.sidecar.util.LRCQueue;
import com.carrotdata.sidecar.util.ScanDetector;
import com.carrotdata.sidecar.util.Statistics;


public class SidecarCachingFileSystem implements SidecarCachingOutputStream.Listener{
  
  
  private static final Logger LOG = LoggerFactory.getLogger(SidecarCachingFileSystem.class);  
  /*
   *  Local cache for remote file data
   */
  private static Cache dataCache; // singleton
  
  /**
   * Data cache type (offheap, file, hybrid)
   */
  
  private static SidecarDataCacheType dataCacheType; 
  
  private static boolean cacheEnabled = true;
  /*
   *  Caches remote file lengths by file path
   */
  private static Cache metaCache; // singleton

  /*
   *  LRU cache for cached on write filenames with their lengths (if enabled) 
   */
  private static LRCQueue<String, Long> writeCacheFileList;
  //private static SizeBasedPriorityQueue writeCacheFileList;
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
   * Thread pool - static
   */
  private static ExecutorService unboundedThreadPool;
  
  /**
   * Blocking queue for above executor service
   * We need direct access to this queue to check its size
   * find tasks and remove them
   */
  private static BlockingQueue<Runnable> taskQueue;
  
  /*
   *  Write cache maximum size (per server instance)
   */
  static long writeCacheMaxSize;
  
  /*
   *  Current write cache size 
   */
  static AtomicLong writeCacheSize = new AtomicLong();
  
  /**
   * Write cache bytes written
   */
  static AtomicLong writeCacheBytesWritten = new AtomicLong();
  
  /**
   * Data set size on disk
   */
  static AtomicLong dataSetSizeOnDisk = new AtomicLong();
  
  /*
   * File eviction thread (from cache-on-write cache)
   */
  static AtomicReference<Thread> evictor = new AtomicReference<>();
  
  /**
   * Write cache URI
   */
  static URI writeCacheURI;
  
  /**
   * Write cache exclude list regex patterns
   */
  static Pattern[] writeCacheExclude;
  
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
   * Write cache mode (DISABLED, SYNC, ASYNC, ASYNC_TURBO)
   */
  private WriteCacheMode writeCacheMode;
  
  /**
   * Data cache mode: ALL, NOT_IN_WRITE_CACHE, SIZE_OVER
   */
  private DataCacheMode dataCacheMode;
  
  /**
   * Minimal file size to cache if data cache mode == SIZE_OVER
   */
  private long minSizeToCache;
  
  /**
   * Is meta data cacheable
   */
  private boolean metaCacheable;
  
  /**
   * Remote FS is not safe, files can be changed
   * So, data pages are always immutable in the cache
   * If file content changes, its modification changes
   * as well. Each data page key depends on combination of
   * remote file path and its modification time. For such 
   * applications set remoteMutable = true
   */
  private boolean remoteMutable;
  
  /**
   * Remote FS URI
   */
  private URI remoteURI;
  
  /**
   * Statistics
   */
  private Statistics stats;
  
  /**
   * Scan detector is enabled on READ
   */
  private boolean scanDetectorEnabled;
  
  /**
   * Scan threshold in data pages
   */
  private int scanThreashold;
  
  /**
   * For testing only
   */
  public static void clear() {
    dataCache = null; // singleton
    metaCache = null; // singleton
  }
  
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

  /**
   * Get write cache maximum size
   * @return size
   */
  public static long getWriteCacheMaxSize() {
    return writeCacheMaxSize;
  }
  
  /**
   * Get current write cache size
   * @return size
   */
  public static long getCurrentWriteCacheSize() {
    return writeCacheSize.get();
  }
  
  /**
   * Get write cache URI
   * @return write cache URI
   */
  public static URI getWriteCacheURI() {
    return writeCacheURI;
  }
  
  /**
   * Get number of files in a write cache (per instance)
   * @return number of files
   */
  public static long getNumberFilesInWriteCache() {
    return writeCacheFileList.size();
  }
  
  /**
   * Pending tasks queue size
   * @return size
   */
  public static int getTaskQueueSize() {
    return taskQueue.size();
  }
  
  /**
   * Get total data size written to the write cache 
   * @return total data size
   */
  public static long getWriteCacheBytesWritten() {
    return writeCacheBytesWritten.get();
  }
  
  /**
   * Get data set size for this server instance
   * @return data set size
   */
  public static long getDataSetSizeOnDisk() {
    return dataSetSizeOnDisk.get();
  }
  
  /**
   * Constructor
   * @param fs remote FS
   */
  private SidecarCachingFileSystem(FileSystem fs) {
    this.remoteFS = fs;
    this.metaCacheable = fs instanceof MetaDataCacheable;
  }

  private void setDataCacheType(CacheConfig config, SidecarDataCacheType type) {
    if (type == SidecarDataCacheType.HYBRID) {
      addCacheType(config, SidecarDataCacheType.MEMORY.getCacheName(), SidecarDataCacheType.MEMORY.getType());
      addCacheType(config, SidecarDataCacheType.FILE.getCacheName(), SidecarDataCacheType.FILE.getType());
    } else if (type != SidecarDataCacheType.DISABLED){
      addCacheType(config, type.getCacheName(), type.getType());
    }
  }
  
  /**
   * Add single cache type (memory or disk)
   * @param confug
   * @param type
   */
  private void addCacheType(CacheConfig config, String cacheName, String type) {
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
   * Is scan detector enabled
   * @return true, false
   */
  public boolean isScanDetectorEnabled() {
    return this.scanDetectorEnabled;
  }
  
  /**
   * Set scan detector enabled
   * @param b enabled or not
   */
  public void setScanDetectorEnabled(boolean b) {
    this.scanDetectorEnabled = b;
  }
  
  /**
   * Get scan detector threshold
   * @return scan detector threshold
   */
  public int getScanDetectorThreshold() {
    return this.scanThreashold;
  }
  
  /**
   * Sets scan detector threshold
   * @param value new threshold
   */
  public void setScanDetectorThreshold(int value) {
    this.scanThreashold = value;
  }
  
  /**
   * Used for testing
   * @param b
   */
  public void setMetaCacheEnabled(boolean b) {
    if (dataCacheType == SidecarDataCacheType.DISABLED) {
      return; // do nothing
    }
    this.metaCacheable = b;
  }
  
  /**
   * Is meta cache enabled
   * @return true or false
   */
  public boolean isMetaCacheEnabled() {
    return this.metaCacheable;
  }
  
  /**
   * Set enable/disable write cache (Eviction thread can temporarily disable write cache)
   * @param b true or false
   */
  public void setWriteCacheEnabled(boolean b) {
    if (this.writeCacheFS == null) {
      return;
    }
    this.writeCacheEnabled = b;
  }
  
  /**
   * Is write cache enabled
   * @return 
   */
  public boolean isWriteCacheEnabled() {
    return this.writeCacheEnabled;
  }
  
  /**
   * Get remote URI
   * @return remote URI
   */
  public URI getURI() {
    return this.remoteURI;
  }
  
  /**
   * Get remote FS URI
   * @return remote URI
   */
  public URI getRemoteFSURI() {
    return getURI();
  }
  
  /**
   * Get write cache FS URI
   * @return write cache URI
   */
  public URI getWriteCacheFSFSURI() {
    if (!writeCacheEnabled) {
      return null;
    }
    return getWriteCacheFS().getUri();
  }
  
  /**
   * Data page size
   * @return size
   */
  public int getDataPageSize() {
    return this.dataPageSize;
  }
  
  /**
   *  Prefetch buffer size
   * @return prefetch buffer size 
   */
  public int getPrefetchBufferSize() {
    return this.ioBufferSize;
  }
  
  /**
   * Get write cache mode
   * @return mode
   */
  public WriteCacheMode getWriteCacheMode() {
    return this.writeCacheMode;
  }
  
  /**
   * Instance statistics
   * @return 
   */
  public Statistics getStatistics() {
    return this.stats;
  }
  
  public void initialize(URI uri, Configuration configuration) throws IOException {
    try {
      if (inited) {
        return;
      }
      
      this.remoteURI = uri;
      
      CacheConfig config = CacheConfig.getInstance();
      Iterator<Map.Entry<String, String>> it = configuration.iterator();
      while (it.hasNext()) {
        Map.Entry<String, String> entry = it.next();
        String name = entry.getKey();
        if (CacheConfig.isCarrotPropertyName(name)) {
          config.setProperty(name, entry.getValue());
        }
      }
      
      final SidecarConfig sconfig = SidecarConfig.fromHadoopConfiguration(configuration);
      dataCacheType = sconfig.getDataCacheType();
      cacheEnabled = dataCacheType != SidecarDataCacheType.DISABLED;
      if (!cacheEnabled) {
        this.metaCacheable = false;
      }
      // Add two caches (sidecar-data, sidecar-meta) types to the configuration
      setDataCacheType(config, dataCacheType);      
      // meta cache is always offheap
      addCacheType(config, META_CACHE_NAME, "memory");
      
      this.dataPageSize = (int) sconfig.getDataPageSize();
      this.ioBufferSize = (int) sconfig.getIOBufferSize();
      this.ioPoolSize = (int) sconfig.getIOPoolSize();
      this.writeCacheMode = sconfig.getWriteCacheMode();
      this.writeCacheEnabled = writeCacheMode != WriteCacheMode.DISABLED;
      this.remoteMutable = sconfig.getRemoteFilesMutable();
      this.dataCacheMode = sconfig.getDataCacheMode();
      this.minSizeToCache = sconfig.getCacheableFileSizeThreshold();
      this.scanDetectorEnabled = sconfig.isScanDetectorEnabled();
      this.scanThreashold = sconfig.getScanDetectorThreshold();
      
      if (this.writeCacheEnabled) {
        writeCacheMaxSize = sconfig.getWriteCacheSizePerInstance();
        URI writeCacheURI = sconfig.getWriteCacheURI();
        if (writeCacheURI != null) {
          // Sanity check
          if(writeCacheURI.getScheme().startsWith("file")) {
            this.writeCacheMode = WriteCacheMode.SYNC;
          }
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

      synchronized (getClass()) {
        if (dataCache == null) {
          writeCacheURI = sconfig.getWriteCacheURI();
          
          initWriteCacheLists(sconfig);
          
          SidecarCachingInputStream.initIOPools(this.ioPoolSize);
          loadDataCache();
          loadMetaCache();
          if (sconfig.doInstallShutdownHook()) {
            // Install shutdown hook if not in a test mode or
            // in test mode with additional config set
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
              try {
                if (sconfig.isCachePersistent()) {
                  saveDataCache();
                  saveMetaCache();
                }
                // we save write cache file list even if persistence == false
                saveStatistics();
                saveWriteCacheFileListCache();
                shutdownExecutorService();
                // TODO: shutdown thread pool
              } catch (IOException e) {
                LOG.error(e.getMessage(), e);
              }
            }));
            if (sconfig.isCachePersistent() && cacheEnabled) {
              LOG.info("Shutdown hook installed for cache[{}]", dataCache.getName());
              LOG.info("Shutdown hook installed for cache[{}]", metaCache.getName());
            }
            LOG.info("Shutdown hook installed for cache[lru-cache]");
          }
          int coreThreads = sconfig.getSidecarThreadPoolMaxSize();
          int keepAliveTime = 60; // hard-coded
          // This is actually unbounded queue (LinkedBlockingQueue w/o parameters)
          // and bounded thread pool - only coreThreads is maximum, maximum number of threads is ignored
          taskQueue = new LinkedBlockingQueue<>();
          unboundedThreadPool = new ThreadPoolExecutor(
            coreThreads, Integer.MAX_VALUE, 
            keepAliveTime, TimeUnit.SECONDS,
            taskQueue,
            BlockingThreadPoolExecutorService.newDaemonThreadFactory(
                "sidecar-thread-pool"));
        }
      }
      loadWriteCacheFileListCache();
      loadStatistics();
      activateJMXSinks();
      this.inited = true;
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e);
    }
  }

  private void initWriteCacheLists(SidecarConfig sconfig) {
    String[] patterns = sconfig.getWriteCacheExcludeList();
    if (patterns.length > 0) {
      writeCacheExclude = new Pattern[patterns.length];
      for (int i = 0; i < patterns.length; i++) {
        writeCacheExclude[i] = Pattern.compile(patterns[i]);
      }
    }
  }
 
  private boolean inExcludeList(String path) {
    for(Pattern p: writeCacheExclude) {
      Matcher m = p.matcher(path);
      if (m.matches()) {
        return true;
      }
    }
    return false;
  }
  
  @SuppressWarnings("unused")
  private boolean doNotCache(Path p) {
    CachingHintDetector detector = CachingHintDetector.fromConfig(SidecarConfig.getInstance());
    boolean result = detector.doNotCache(false);
    if (result) {
      LOG.error("Not caching current output {}", p);
    } else {
      LOG.error("Flush detected {}", p);
    }
    return result;
  }
  
  private boolean isCacheable(Path p) {
    return !(inExcludeList(p.toString()));// || doNotCache(p));
  }
  private void activateJMXSinks() {
    SidecarConfig sconfig = SidecarConfig.getInstance();
    if (sconfig.isJMXMetricsEnabled()) {
      LOG.info("SidecarCachingFileSystem JMX enabled for sidecar");
      String domainName = sconfig.getJMXMetricsDomainName();
      String mtype = this.remoteURI.toString();
      mtype = mtype.replace(":", "-");
      mtype = mtype.replace("/", "");
      registerSiteJMXMetricsSink(domainName);
      registerSidecarJMXMetricsSink(domainName, mtype);
    }      
  }

  public void registerSiteJMXMetricsSink(String domainName) {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
    ObjectName name;
    try {
      name = new ObjectName(String.format("%s:type=Site,name=WriteCache",domainName));
      SidecarSiteJMXSink mbean = new SidecarSiteJMXSink();
      mbs.registerMBean(mbean, name); 
    } catch (Exception e) {
      LOG.error("Failed", e);
    }
  }
  
  public void registerSidecarJMXMetricsSink(String domainName, String type) {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
    ObjectName name;
    try {
      name = new ObjectName(String.format("%s:type=%s,name=Statistics",domainName, type));
      SidecarJMXSink mbean = new SidecarJMXSink(this);
      mbs.registerMBean(mbean, name); 
    } catch (Exception e) {
      LOG.error("Failed", e);
    }
  }
  
  private void shutdownExecutorService() {
    unboundedThreadPool.shutdownNow();
    boolean result = false;
    while (!result) {
      try {
        // Unsafe operations: COPY, CLOSE (COPY)
        // Is it safe interrupt them?
        // Are they idempotent?
        result = unboundedThreadPool.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.interrupted();
      }
      LOG.info("Shutting down the executor service, task queue length={}", taskQueue.size() );
    }
  }
 
  private boolean inWriteCache(Path p) {
    if (!writeCacheEnabled) {
      return false;
    }
    boolean result = writeCacheFileList.exists(p.toString());
    if (result) {
      this.stats.addTotalFilesOpenedInWriteCache(1);
    }
    return result;
  }
  
  private boolean isCacheableFile(Path p, long size) {
    p = writeCacheEnabled? remoteToCachingPath(p): p;
    boolean inWC = inWriteCache(p);
    switch(dataCacheMode) {
      case ALL: return true;
      case NOT_IN_WRITE_CACHE: return writeCacheEnabled ? !inWC: true;
      case MINSIZE: return size > this.minSizeToCache || writeCacheEnabled && !inWC;
    }
    return true;
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
  
  private String uriToFileSafeName(URI uri) {
    String scheme = uri.getScheme();
    String host = uri.getHost();
    String path = uri.getPath();
    if (host != null) {
      scheme += "-" + host;
    }
    if (path != null) {
      scheme += "-" + path;
    }
    return scheme;
  }
  
  private void loadWriteCacheFileListCache() throws IOException {
    if (!this.writeCacheEnabled) {
      LOG.info("Skipping write-cache-file-list loading, write cache is disabled");
      return;
    }
    CacheConfig config = CacheConfig.getInstance();
    String snapshotDir = config.getSnapshotDir(LRCQueue.NAME);
    String fileName = snapshotDir + File.separator + LRCQueue.FILE_NAME;
    File file = new File(fileName);
    writeCacheFileList = new LRCQueue<>();

    if (file.exists()) {
      FileInputStream fis = new FileInputStream(file);
      DataInputStream dis = new DataInputStream(fis);
      writeCacheFileList.load(dis);
      dis.close();
      LOG.info("Loaded cache[{}]", LRCQueue.NAME);
    } else {
      LOG.info("Created new cache[{}]", LRCQueue.NAME);
    }
  }

  private void loadStatistics() throws IOException {
    
    CacheConfig config = CacheConfig.getInstance();
    String snapshotDir = config.getSnapshotDir(uriToFileSafeName(remoteURI));
    String fileName = snapshotDir + File.separator + "sidecar.stats";
    File file = new File(fileName);
    this.stats = new Statistics();

    if (file.exists()) {
      FileInputStream fis = new FileInputStream(file);
      DataInputStream dis = new DataInputStream(fis);
      // Write cache
      writeCacheSize.set(dis.readLong());
      writeCacheBytesWritten.set(dis.readLong());
      dataSetSizeOnDisk.set(dis.readLong());
      // General
      this.stats.load(dis);
      dis.close();
      LOG.info("Loaded sidecar sidecar.stats");
    } else {
      LOG.info("Created new sidecar staistics");
    }
  }
  
  private void saveStatistics() throws IOException {

    CacheConfig config = CacheConfig.getInstance();
    String snapshotDir = config.getSnapshotDir(uriToFileSafeName(remoteURI));
    String fileName = snapshotDir + File.separator + "sidecar.stats";
    File file = new File(fileName);

    FileOutputStream fos = new FileOutputStream(file);
    DataOutputStream dos = new DataOutputStream(fos);
    // Write cache
    dos.writeLong(writeCacheSize.get());
    dos.writeLong(writeCacheBytesWritten.get());  
    dos.writeLong(dataSetSizeOnDisk.get());
    // General
    this.stats.save(dos);
    dos.close();
    LOG.info("Saved sidecar sidecar.stats");
  }
  
  private Cache loadMetaCache() throws IOException{
    if (dataCacheType == SidecarDataCacheType.DISABLED) {
      return null;
    }
    CacheConfig config = CacheConfig.getInstance();
    SidecarConfig sconfig = SidecarConfig.getInstance();
    try {
      long maxSize = config.getCacheMaximumSize(META_CACHE_NAME);
      int dataSegmentSize = (int) config.getCacheSegmentSize(META_CACHE_NAME);
      metaCache = Cache.loadCache( META_CACHE_NAME);
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
          Arrays.toString(config.getCacheRootDirs(metaCache.getName())));      }
    } catch (IOException e) {
      LOG.error("loadMetaCache error", e);
      throw e;
    }     
    if (sconfig.isJMXMetricsEnabled()) {
      LOG.info("SidecarCachingFileSystem JMX enabled for meta-data cache");
      String domainName = sconfig.getJMXMetricsDomainName();
      metaCache.registerJMXMetricsSink(domainName);
    }  
    return metaCache;
  }

  private Cache loadDataCache(SidecarDataCacheType type) throws IOException {
    if (type == SidecarDataCacheType.DISABLED) {
      return null;
    }
    CacheConfig config = CacheConfig.getInstance();
    boolean isPersistent = SidecarConfig.getInstance().isCachePersistent();
    Cache dataCache = null;
    if (isPersistent) {
      try {
        dataCache = Cache.loadCache(type.getCacheName());
        if (dataCache != null) {
          LOG.info("Loaded cache[{}] from the path: {}", dataCache.getName(),
            Arrays.toString(config.getCacheRootDirs(dataCache.getName())));
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
    if (dataCacheType == SidecarDataCacheType.DISABLED) {
      return null;
    }
    if (dataCacheType != SidecarDataCacheType.HYBRID) {
      dataCache = loadDataCache(dataCacheType);
      return dataCache;
    } else {
      dataCache = loadDataCache(SidecarDataCacheType.MEMORY);
      Cache victimCache = loadDataCache(SidecarDataCacheType.FILE);
      dataCache.setVictimCache(victimCache);
      return dataCache;
    }
  }
  
  void saveWriteCacheFileListCache() throws IOException {
    if (writeCacheFileList != null) {
      long start = System.currentTimeMillis();
      LOG.info("Shutting down cache[{}]", LRCQueue.NAME);
      CacheConfig config = CacheConfig.getInstance();
      String snapshotDir = config.getSnapshotDir(LRCQueue.NAME);
      FileOutputStream fos = new FileOutputStream(snapshotDir + 
      File.separator + LRCQueue.FILE_NAME);
      // Save total write cache size
      DataOutputStream dos = new DataOutputStream(fos);
      writeCacheFileList.save(dos);
      // do not close - it was closed already
      long end = System.currentTimeMillis();
      LOG.info("Shutting down cache[{}] done in {}ms",LRCQueue.NAME , (end - start));
    }
  }

  void saveDataCache() throws IOException {
    if (dataCacheType == SidecarDataCacheType.DISABLED) {
      return;
    }
    long start = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}]", DATA_CACHE_FILE_NAME);
    dataCache.shutdown();
    long end = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}] done in {}ms", DATA_CACHE_FILE_NAME, (end - start));
  }
  
  void saveMetaCache() throws IOException {
    if (dataCacheType == SidecarDataCacheType.DISABLED) {
      return;
    }
    long start = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}]", META_CACHE_NAME);
    metaCache.shutdown();
    long end = System.currentTimeMillis();
    LOG.info("Shutting down cache[{}] done in {}ms", META_CACHE_NAME, (end - start));
  }
  
  public static void dispose() {
    if (dataCacheType != SidecarDataCacheType.DISABLED) {
      dataCache.dispose();
      metaCache.dispose();
    }
    if (cachedFS != null) {
      cachedFS.clear();
    }
    dataCache = null;
    metaCache = null;
    writeCacheSize.set(0);
  }

  Path remoteToCachingPath(Path remotePath) {
    remotePath = isQualified(remotePath)? remotePath: 
      this.remoteFS.makeQualified(remotePath);
    URI remoteURI = remotePath.toUri();
    String path = remoteURI.getPath();
    String host = remoteURI.getHost();
    String scheme = remoteURI.getScheme();
    if (host != null) {
      path = Path.SEPARATOR + scheme + Path.SEPARATOR + host + Path.SEPARATOR + path;
    }
    Path cacheWorkDir = writeCacheFS.getWorkingDirectory();
    String fullPath = cacheWorkDir + path;
    Path cachePath =  new Path(fullPath);  
    return writeCacheFS.makeQualified(cachePath);
  }
  
  Path cachingToRemotePath(Path cachingPath) {
    URI cachingURI = cachingPath.toUri();
    String path = cachingURI.getPath();
    URI cPath = writeCacheFS.getWorkingDirectory().toUri();
    String cachePath = cPath.getPath();
    int index = path.indexOf(cachePath);
    URI remoteURI = this.remoteFS.getUri();
    String host = remoteURI.getHost();
    String scheme = remoteURI.getScheme();
    int len = host.length() + scheme.length() + 2;
    String relativePath = path.substring(index + cachePath.length() + len);
    if (!relativePath.startsWith(File.separator)) {
      relativePath = File.separator + relativePath;
    }
    return this.remoteFS.makeQualified(new Path(relativePath));
  }

  
  /**
   * Single thread only, but I think we are OK?
   */
  private void checkEviction() {
    double storageOccupied = (double) writeCacheSize.get() / writeCacheMaxSize;
    if (storageOccupied > writeCacheEvictionStartsAt) {
      LOG.debug("checkEviction storage occupied={}", storageOccupied);
      if (writeCacheFileList.size() == 0) {
        // Wait until first file be available
        // This can happen when 
        //TODO: make this log message periodic with some interval
        LOG.warn("No files to evict from write cache found. Write cache size {} is too small.", writeCacheMaxSize);
        return;
      }
      Thread t = evictor.get();
      if (t != null && t.isAlive()) {
        return ; // eviction is in progress
      }
      // else
      t = new Thread(() -> {
        try {
          // Wait 10 seconds until (possibly) clean up after compaction
          Thread.sleep(10000);
          evictFiles();
        } catch (Exception e) {
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
   * @param p file path (expect fully qualified)
   * @return true - yes, false - otherwise
   * @throws IOException 
   */
  final boolean metaExists(Path p) throws IOException {
    if (!metaCacheable) {
      return false;
    }
    p = isQualified(p)? p: this.remoteFS.makeQualified(p);
    byte[] hashedKey = hashCrypto(p.toString().getBytes());
    boolean b = metaExists(hashedKey);
    return b;
  }
  
  private boolean metaExists(byte[] key) throws IOException {
    if (!metaCacheable) {
      return false;
    }
    return metaCache.existsExact(key, 0, key.length);
  }
  /**
   * Put meta (currently, only file length later will add modification time)
   * @param p file path expects fully qualified
   * @param status file status
   * @return true on success, false - otherwise
   */
  final boolean metaPut(Path p, FileStatus status) {
    if (!metaCacheable) {
      return false;
    }
    p = isQualified(p)? p: this.remoteFS.makeQualified(p);
    byte[] hashedKey = hashCrypto(p.toString().getBytes());
    return metaPut(hashedKey, status);
  }
  
  /**
   * TODO: do not ignore failed put - check operation result
   * @param key hashed key for a path
   * @param status file status
   * @return true or false
   */
  private boolean metaPut(byte[] key, FileStatus status) {
    if (!metaCacheable) {
      return false;
    }
    try {
      byte[] buf = new byte[17];
      UnsafeAccess.putLong(buf, 0, status.getModificationTime());
      UnsafeAccess.putLong(buf, 8, status.getLen());
      buf[16] = status.isDirectory()? (byte) 1: (byte) 0;
      return metaCache.put(key, buf, 0);
    } catch (IOException e) {
      //TODO: proper error handling
      LOG.error("Can not save file meta", e);
    }
    return false;
  }
  
  final FileStatus metaGet (Path p) {
    if (!metaCacheable) {
      return null;
    }
    p = isQualified(p)? p: this.remoteFS.makeQualified(p);
    byte[] hashedKey = hashCrypto(p.toString().getBytes());
    byte[] data =  metaGet(hashedKey);
    if (data == null) {
      return null;
    }
    long modTime = UnsafeAccess.toLong(data, 0);
    long length = UnsafeAccess.toLong(data, 8);
    boolean isDir = data[16] == 0? false: true;
    return new CachedFileStatus(remoteFS, p, modTime, length, isDir);
  }

  /**
   * Length of a file or -1
   * @param key file key (hashed path value to 16 bytes crypto MD5)
   * @return
   */
  private byte[] metaGet(byte[] key) {
    if (!metaCacheable) {
      return null;
    }
    // 64 is sufficient to keep the whole key (16) + 
    // value (8 modification time, 8 - length, 1 byte is directory) 
    byte[] buf = new byte[64];
    try {
      int size = (int) metaCache.get(key,  0,  key.length, buf, 0);
      if (size < 0) {
        // does not exist in the meta cache
        return null;
      }
      return buf;
      
    } catch (IOException e) {
    //TODO: proper error handling
      LOG.error("Can not get file meta", e);
    }
    return null;
  }
  
  final boolean metaDelete(Path p) {
    if (!metaCacheable) {
      return false;
    }
    p = isQualified(p)? p: this.remoteFS.makeQualified(p);
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
   * @param status file status
   * @throws IOException 
   */
  final void metaSave(Path path, FileStatus status) throws IOException {
    if (metaCacheable) {
      if (!metaExists(path)) {
        boolean result = metaPut(path, status);
        if (!result) {
          LOG.error("Failed to save meat for {}", path);
        }
      }
    }
  }
  
  /**
   * Update meta with a new object
   * @param path file path
   * @param newStatus file status
   */
  final void metaUpdate(Path path, FileStatus newStatus) {
    if (!metaCacheable) {
      return;
    }
    metaDelete(path);
    metaPut(path, newStatus);
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
  final void dataDeleteFile(Path path, FileStatus status) {
    if (status == null || !cacheEnabled) {
      return;
    }
    final Path qualified = isQualified(path)? path: this.remoteFS.makeQualified(path);
    LOG.debug("Evict data pages for {} length={}", path, status.getLen());

    long size = status.getLen();
    byte[] baseKey = getBaseKey(qualified, status.getModificationTime());
    long off = 0;
    while (off < size) {
      byte[] key = getKey(baseKey, off, dataPageSize);
      try {
        boolean res = dataCache.delete(key);
        LOG.debug("Delete {} result={}", off, res);
      } catch (IOException e) {
        LOG.error("Evictor failed", e);
        return;
      }
      off += dataPageSize;
    }
  }
  
  /*********************************
   * 
   * Data cache access end
   *********************************/
  private boolean isQualified(Path p) {
    return p.toUri().getScheme() != null;
  }
  
  @Override
  public void bytesWritten(SidecarCachingOutputStream stream, long bytes) {
    if (isWriteCacheEnabled() && stream.getCachingStream() != null) {
      writeCacheSize.addAndGet(bytes);
      writeCacheBytesWritten.addAndGet(bytes);
      //checkEviction();
    }
  }

  @Override
  public void closingRemote(SidecarCachingOutputStream stream) {     
    final Path path = this.remoteFS.makeQualified(stream.getRemotePath());
    long length = 0;
    this.stats.addTotalFilesCreated(1);
    try {
      length = stream.length();
    } catch(IOException e) {
      LOG.error("Remote stream getPos() failed {}", e);
    }
    LOG.debug("Closing remote {} len={}", path, length);
    
    if (writeCacheEnabled && stream.getCachingStream() != null) {
   // Update data set size
      dataSetSizeOnDisk.addAndGet(length);
      // It is save to update meta first in the cache
      // because write cache has already file ready to read
      FileStatus status = new CachedFileStatus(remoteFS, path, 0, length, false);
      try {
        metaSave(path, status);
      } catch (IOException e) {
        //FIXME
        LOG.error("Closing remote stream", e);
        return;
      }
      // ASYNC
      Path cachePath = remoteToCachingPath(path);
      writeCacheFileList.put(cachePath.toString(), length);
      Runnable r = () -> {
        try {
          FSDataOutputStream os = stream.getRemoteStream();
          os.close();
          deleteMoniker(remoteToCachingPath(path));
          checkEviction();
        } catch (IOException e) {
          // TODO - how to handle exception?
          LOG.error("Closing remote stream", e);
        }
      };
      if (writeCacheMode != WriteCacheMode.SYNC) {
        unboundedThreadPool.submit(r);
      } else {
        //SYNC mode
        r.run();
      }
    } else {
      //SYNC
      try {
        FSDataOutputStream os = stream.getRemoteStream();
        // This is where all actual data transmission start
        os.close();
        // now update meta
        FileStatus status = new CachedFileStatus(remoteFS, path, 0, length, false);
        metaSave(path, status);
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
  
  void evictDataPages(Path path, FileStatus status) {
    if (status == null) {
      LOG.debug("evictDataPages: file {} path is not in the meta cache", path);
      return;
    }
    dataDeleteFile(path, status);
  }
  
  /**
   * For testing only - not visible outside the package
   * @param remotePath
   * @return true on success, false - otherwise
   * @throws IOException
   */
  boolean deleteFromWriteCache(Path remotePath) throws IOException {
    if (this.writeCacheFS == null) {
      return false;
    }
    metaDelete(remotePath);
    Path p = remoteToCachingPath(remotePath);
    writeCacheFileList.remove(p.toString());
    return this.writeCacheFS.delete(p, false);
  }
  
  private boolean isFile(Path p) throws IOException {
    boolean result;
    //TODO: is this correct?
    if (metaExists(p)) {
      // we keep only files in meta
      return true;
    }
    FileStatus fs = remoteFS.getFileStatus(p);
    result = fs.isFile();
    if (result && metaCacheable) {
      metaSave(p, fs);
    }
    return result;
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
    LOG.debug("EVICTOR Write cache file number={} write cache size={} writeCacheMaxSize={}",
      writeCacheFileList.size(), writeCacheSize.get(), writeCacheMaxSize );
    try {
      while (usedStorageRatio > writeCacheEvictionStopsAt) {
        String fileName = writeCacheFileList.evictionCandidate();
        if (fileName == null) {
          LOG.error("EVICTOR Write cache file list is empty");
          return;
        }
        Long len = writeCacheFileList.get(fileName);
        FileStatus status = metaGet(cachingToRemotePath(new Path(fileName)));
        if (status != null) {
         /*DEBUG*/ LOG.debug("EVICTOR Evict file {} len={} created {} seconds ago", fileName, len, 
            (System.currentTimeMillis() - status.getModificationTime()) / 1000);
        } else {
          /*DEBUG*/ LOG.debug("EVICTOR Evict file {} len={}", fileName, len);
        }
        Path p = new Path(fileName);
        boolean exists = this.writeCacheFS.exists(p);
        Path moniker = getFileMonikerPath(p);
        if (this.writeCacheFS.exists(moniker)) {
          // Disable write cache temporarily
          setWriteCacheEnabled(false);
          // Log warning
          LOG.error("EVICTOR Disable write cache, eviction candidate {} has not been synced to remote yet", p);
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
            LOG.debug("EVICTOR Write Cache #files={} cache size={}", writeCacheFileList.size(), writeCacheSize.get());
          } else {
            LOG.error("EVICTOR failed to delete {}", fileName);
          }
        } catch (IOException e) {
          LOG.error("EVICTOR File evictor failed to delete file {}", fileName);
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
  
  public static LRCQueue<String,Long> getWriteCacheFileListCache() {
    return writeCacheFileList;
  }

//  public static SizeBasedPriorityQueue getWriteCacheFileListCache() {
//    return writeCacheFileList;
//  }

  public static void clearFSCache() {
    cachedFS.clear();
  }
  
  public void shutdown() throws IOException {
    shutdown(true);
  }
  
  public void shutdown(boolean dispose) throws IOException {
    boolean isPersistent = SidecarConfig.getInstance().isCachePersistent();
    if (isPersistent) {
      saveDataCache();
      saveMetaCache();
    }
    saveWriteCacheFileListCache();
    clearFSCache();
    if (dispose) {
      shutdownExecutorService();
      dispose();
    }
  }
  
  /**
   * Are files mutable in remote FS (FS supports append, or applications can overwrite files)
   * @return true or false
   */
  public boolean isMutableFS() {
    return this.remoteMutable;
  }
  
  /**********************************************************************************
   * 
   * Hadoop FileSystem API
   * 
   **********************************************************************************/
  
  private boolean isPotentiallyMutable(Path p) {
    return this.remoteMutable;
  }
  
  /**
   * Get file status. It tries met cache first
   * @param p file path
   * @return file status
   * @throws IOException
   */
  public FileStatus getFileStatus(Path p) throws IOException {
    final Path qualifiedPath = isQualified(p)? p : 
      this.remoteFS.makeQualified(p);
    boolean mutable = isPotentiallyMutable(qualifiedPath);
    FileStatus cached = metaGet(qualifiedPath);
    if (mutable || cached == null) {
      FileStatus status = ((RemoteFileSystemAccess) remoteFS).getFileStatusRemote(qualifiedPath);
      long cachedModTime = cached != null? cached.getModificationTime(): 0;
      if (status.getModificationTime() != cachedModTime || cached == null) {
        // Delete old
        metaUpdate(qualifiedPath, status);
        Runnable r = () -> {
          dataDeleteFile(qualifiedPath, cached);
        };
        unboundedThreadPool.submit(r);
      }
      return status;
    } else {
      // mutable == false && cached != null
      return cached; 
    }
  }
  /**
   * Concatenate existing files together.
   * Some File Systems (ADL Gen1) supports this API
   * @param trg the path to the target destination.
   * @param psrcs the paths to the sources to use for the concatenation.
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default).
   */
  public void concat(final Path trg, final Path [] psrcs) throws IOException {
    LOG.debug("Concat to {} files {}", com.carrotdata.sidecar.util.Utils.join(psrcs, "\n"));
    if (writeCacheEnabled) {
      // Caching file system does not support concat?
      // HDFS - does not support - so delete files from caching
      //TODO: fix it - check concatenation first, then delete if exception
      for (Path p : psrcs) {
        Path cachedPath = remoteToCachingPath(p);
        // Remove from write-cache file list
        writeCacheFileList.remove(cachedPath.toString());
        // Delete file
        writeCacheFS.delete(cachedPath, false);
      }
    }
    // Clear data cache asynchronously
    Runnable r = () -> {
      for (Path p: psrcs) {
        FileStatus fs = metaGet(p);
        if (fs != null) {
          metaDelete(p);
          evictDataPages(p, fs);
        }
      }
    };
    unboundedThreadPool.submit(r);
    ((RemoteFileSystemAccess)remoteFS).concatRemote(trg, psrcs);
  }
  
  /**
   * Open file 
   * @param path path to the file
   * @param bufferSize buffer size
   * @return input stream
   * @throws Exception
   */
    
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {

    final Path qPath = isQualified(path)? path: this.remoteFS.makeQualified(path);
    Callable<FSDataInputStream> remoteCall = () -> {
      return ((RemoteFileSystemAccess)remoteFS).openRemote(qPath, bufferSize);
    };
    Callable<FSDataInputStream> cacheCall = () -> {return null;};
    if (writeCacheEnabled) {
      Path writeCachePath = remoteToCachingPath(path);
      cacheCall = () -> {
        return writeCacheFS.open(writeCachePath, bufferSize);
      };
    }
    FileStatus status = getFileStatus(qPath);
    long length = status.getLen();
    boolean cacheOnRead = isCacheableFile(qPath, length);
    ScanDetector sd = cacheOnRead && isScanDetectorEnabled()? 
        new ScanDetector(scanThreashold, dataPageSize): null;
    SidecarCachingInputStream scis = new SidecarCachingInputStream(dataCache, status, remoteCall, cacheCall,
      dataPageSize, ioBufferSize, this.stats, cacheOnRead, sd);
    FSDataInputStream cachingInputStream = new FSDataInputStream(scis);
    this.stats.addTotalFilesOpened(1);
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
  
    LOG.debug("Sidecar create file: {}", f);
    FSDataOutputStream remoteOut = 
       ((RemoteFileSystemAccess)remoteFS).createRemote(f, permission, overwrite, bufferSize, 
         replication, blockSize, progress);
    
    boolean exclude = !isCacheable(f);// inExcludeList(f.toString());
    if (exclude && writeCacheEnabled) {
      LOG.debug("FOUND write cache exclude {}", f);
    }
    
    FSDataOutputStream cacheOut = null;
    if (writeCacheEnabled && !exclude) {
      Path cachePath = remoteToCachingPath(f);
      try {
        cacheOut = this.writeCacheFS.create(cachePath, permission, overwrite, bufferSize, replication,
          blockSize, null);
      } catch (IOException e) {
        LOG.error("Write cache create file failed", e);
        return remoteOut;
      }
      // Create file moniker
      createMoniker(cachePath);
    }
    return new FSDataOutputStream(new SidecarCachingOutputStream(cacheOut, remoteOut, f, this));
  }
  
  @SuppressWarnings("deprecation")
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> cflags,
      int bufferSize, short replication, long blockSize, Progressable progress,
      ChecksumOpt checksumOpt) throws IOException {
    FSDataOutputStream remoteOut = 
       ((RemoteFileSystemAccess)remoteFS).createRemote(f, permission, cflags, bufferSize, 
         replication, blockSize, progress, checksumOpt);
    boolean exclude = !isCacheable(f);// inExcludeList(f.toString());
    if (exclude && writeCacheEnabled) {
      LOG.debug("Create: found write cache exclude {}", f);
    }
    
    FSDataOutputStream cacheOut = null;
    if (writeCacheEnabled && !exclude) {
      Path cachePath = remoteToCachingPath(f);
      try {
        cacheOut = this.writeCacheFS.create(cachePath, permission, cflags, bufferSize, replication,
          blockSize, null);
      } catch (IOException e) {
        LOG.error("Write cache create file failed", e);
        return remoteOut;
      }
      // Create file moniker
      createMoniker(cachePath);
    }
    return new FSDataOutputStream(new SidecarCachingOutputStream(cacheOut, remoteOut, f, this));
  }
  
  public boolean mkdirs(Path path, FsPermission permission)
      throws IOException, FileAlreadyExistsException {
    LOG.debug("Create dir: {}", path);
    boolean result = ((RemoteFileSystemAccess)remoteFS).mkdirsRemote(path, permission);
    if (result && this.writeCacheFS != null) {
      Path cachedPath = remoteToCachingPath(path);
      this.writeCacheFS.mkdirs(cachedPath); // we use default permission
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
        ((RemoteFileSystemAccess)remoteFS).createNonRecursiveRemote(path, permission, flags, bufferSize, 
          replication, blockSize, progress);
    boolean exclude = !isCacheable(path); //inExcludeList(path.toString());
    if (exclude && writeCacheEnabled) {
      LOG.debug("Create: found write cache exclude {}", path);
    }
    FSDataOutputStream cacheOut = null;
    if (writeCacheEnabled && !exclude) {
      Path cachePath = remoteToCachingPath(path);
      try {
        cacheOut = this.writeCacheFS.create(cachePath, true, bufferSize, replication, blockSize);
      } catch(IOException e) {
        LOG.error("Write cache create file failed", e);
        return remoteOut;
      }
      // Create file moniker
      createMoniker(cachePath);
    }
    return new FSDataOutputStream(new SidecarCachingOutputStream(cacheOut, remoteOut, path, this));
  }

  @SuppressWarnings("deprecation")
  public FSDataOutputStream createNonRecursive(Path path, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    
    LOG.debug("Create non-recursive {}", path);
    FSDataOutputStream remoteOut = 
        ((RemoteFileSystemAccess)remoteFS).createNonRecursiveRemote(path, permission, overwrite, bufferSize, 
          replication, blockSize, progress);

    boolean exclude = !isCacheable(path); //inExcludeList(path.toString());
    if (exclude && writeCacheEnabled) {
      LOG.debug("Create: found write cache exclude {}", path);
    }

    FSDataOutputStream cacheOut = null;
    if (writeCacheEnabled && !exclude) {
      Path cachePath = remoteToCachingPath(path);
      try {
        cacheOut = this.writeCacheFS.create(cachePath, true, bufferSize, replication, blockSize);
      } catch (IOException e) {
        LOG.error("Write cache create file failed", e);
        return remoteOut;
      }
      // Create file moniker
      createMoniker(cachePath);
    }
    return new FSDataOutputStream(new SidecarCachingOutputStream(cacheOut, remoteOut, path, this));
  }
  
  @SuppressWarnings("deprecation")
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    // Object storage FS do not support this operation, at least S3
    LOG.debug("Append {}", f);
    FSDataOutputStream remoteOut = ((RemoteFileSystemAccess)remoteFS).appendRemote(f, bufferSize, progress);

    // Usually append is not supported by cloud object stores
    if (!this.writeCacheEnabled) {
      return remoteOut;
    }
    boolean exclude = !isCacheable(f);//inExcludeList(f.toString());
    if (exclude && writeCacheEnabled) {
      LOG.debug("Create: found write cache exclude {}", f);
    }
    FSDataOutputStream cacheOut = null;
    if (writeCacheEnabled && !exclude) {
      Path cachePath = remoteToCachingPath(f);
      try {
        cacheOut = this.writeCacheFS.append(cachePath, bufferSize);
      } catch(IOException e) {
        LOG.error("Write cache create file failed", e);
        return remoteOut;
      }
      // Create file moniker
      createMoniker(cachePath);
    }
    return new FSDataOutputStream(new SidecarCachingOutputStream(cacheOut, remoteOut, f, this));
  }

  public boolean rename(Path src, Path dst) throws IOException {
    LOG.debug("Rename {}\n to {}", src, dst);
    // Check if src is file
    boolean isFile = isFile(src);
    boolean result = ((RemoteFileSystemAccess) remoteFS).renameRemote(src, dst);
    boolean delete = !isCacheable(dst);// inExcludeList(dst.toString());
    if (delete && writeCacheEnabled) {
        LOG.debug("Rename exclude found to {}", dst);
    }
    if (result && isFile) {
      FileStatus status = metaGet(src);
      if (status != null) {
        metaDelete(src);
        if (!delete) {
          metaPut(dst, status);
        }
      }
      // Clear data pages cache
      Runnable r = () -> {
        evictDataPages(src, status);
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
          boolean res = delete? this.writeCacheFS.delete(cacheSrc, false):
              this.writeCacheFS.rename(cacheSrc, cacheDst);
          if (res) {
            // Remove from write-cache file list
            Long len = writeCacheFileList.remove(cacheSrc.toString());
            if (delete) {
              //TODO: check len is not NULL?
              writeCacheSize.addAndGet(-len);
            }
            if (!delete && len != null) {
              writeCacheFileList.put(cacheDst.toString(), len);
            }
          }
        }
      } catch (IOException e) {
        LOG.error(String.format("Failed to rename {} to {}", cacheSrc, cacheDst), e);
      }
    }
    return result;
  }

  public void rename(Path src, Path dst, Rename... options) throws IOException {
    LOG.debug("Rename with options {}\n to {}", src, dst);
    // Check if src is file
    boolean isFile = isFile(src);
    ((RemoteFileSystemAccess) remoteFS).renameRemote(src, dst, options);
    boolean delete = !isCacheable(dst);// inExcludeList(dst.toString());
    if (delete && writeCacheEnabled) {
      LOG.debug("Rename exclude found {}", dst);
    }
    if (isFile) {
      FileStatus status = metaGet(src);
      if (status != null) {
        metaDelete(src);
        if (!delete) {
          metaPut(dst, status);
        }
      }
      // Clear data pages cache
      Runnable r = () -> {
        evictDataPages(src, status);
        // Update meta cache
      };
      unboundedThreadPool.submit(r);
      if (!this.writeCacheEnabled) {
        return ;
      }

      Path cacheSrc = null, cacheDst = null;
      try {
        cacheSrc = remoteToCachingPath(src);
        cacheDst = remoteToCachingPath(dst);
        if (this.writeCacheFS.exists(cacheSrc)) {
          boolean res = delete? writeCacheFS.delete(cacheSrc, false): writeCacheFS.rename(cacheSrc, cacheDst);
          if (res) {
            // Remove from write-cache file list
            Long len = writeCacheFileList.remove(cacheSrc.toString());
            if (delete) {
              writeCacheSize.addAndGet(-len);
            }
            if (!delete && len != null) {
              writeCacheFileList.put(cacheDst.toString(), len);
            }
          }
        }
      } catch (IOException e) {
        LOG.error(String.format("Failed to rename {} to {}", cacheSrc, cacheDst), e);
      }
    }
  }

  public boolean delete(Path f, boolean recursive) throws IOException {

    LOG.debug("Delete {} recursive={}", f, recursive);
    FileStatus status = metaGet(f);
    boolean result = ((RemoteFileSystemAccess)remoteFS).deleteRemote(f, recursive);
    if (status != null) {
      dataSetSizeOnDisk.addAndGet(-status.getLen());
    }
    if (result) {
      this.stats.addTotalFilesDeleted(1);
    }
    
    // Check if f is file
    if (status != null && result) {
      // Clear data pages cache
      Runnable r = () -> {
        evictDataPages(f, status);
        metaDelete(f);
      };
      unboundedThreadPool.submit(r);
    } 

    if (this.writeCacheEnabled) {
      Path p = remoteToCachingPath(f);
      try {
        // Delete from cache file list
        writeCacheFileList.remove(p.toString());
        boolean res = this.writeCacheFS.delete(p, recursive);
        LOG.debug("DELETE {} result={} len={}", f, res, status == null? -1: status.getLen());
        if (res && status != null) {
          // it can be directory
          writeCacheSize.addAndGet(-status.getLen());
        }
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
