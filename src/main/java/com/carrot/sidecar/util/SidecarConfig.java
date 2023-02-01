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
package com.carrot.sidecar.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SidecarConfig extends Properties {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SidecarConfig.class);
  

  public final static String DATA_CACHE_FILE_NAME = "sidecar-data-file";
  public final static String DATA_CACHE_OFFHEAP_NAME = "sidecar-data-offheap";
  
  public final static String META_CACHE_NAME = "sidecar-meta-offheap";
  
  public final static String SIDECAR_WRITE_CACHE_ENABLED_KEY = "sidecar.write.cache.enabled";
  
  public final static String SIDECAR_WRITE_CACHE_URI_KEY = "sidecar.write.cache.uri";
  
  /** This is not a global size, but per server instance */
  public final static String SIDECAR_WRITE_CACHE_SIZE_KEY = "sidecar.write.cache.size";
  
  public final static String SIDECAR_DATA_PAGE_SIZE_KEY = "sidecar.data.page.size";
  
  public final static String SIDECAR_IO_BUFFER_SIZE_KEY = "sidecar.io.buffer.size";
  
  public final static String SIDECAR_IO_POOL_SIZE_KEY = "sidecar.io.pool.size";
  
  public final static String SIDECAR_JMX_METRICS_ENABLED_KEY ="sidecar.jmx.metrics.enabled";
  
  public final static String SIDECAR_JMX_METRICS_DOMAIN_NAME_KEY ="sidecar.jmx.metrics.domain.name";
  
  public final static String SIDECAR_TEST_MODE_KEY = "sidecar.test.mode"; 
  
  public final static String SIDECAR_DATA_CACHE_TYPE_KEY = "sidecar.data.cache.type";
  
  public final static String SIDECAR_PERSISTENT_CACHE_KEY = "sidecar.cache.persistent";
  
  /**
   * Comma-separated list of regular expressions of directory names in
   * remote file system, which must be excluded from caching 
   */
  public final static String SIDECAR_EXCLUDE_PATH_LIST_KEY = "sidecar.exclude.path.list";
  
  /**
   * Comma-separated list of regular expressions of directory names in
   * remote file system, which must be included into caching. All other paths will 
   * be ignored, as well as those defined in in the exclude list
   * One should specify either exclude list or include list, not both
   */
  public final static String SIDECAR_INCLUDE_PATH_LIST_KEY = "sidecar.include.path.list";

  public final static boolean DEFAULT_SIDECAR_WRITE_CACHE_ENABLED = false;
  
  public final static long DEFAULT_SIDECAR_WRITE_CACHE_SIZE = 0;
  
  public final static long DEFAULT_SIDECAR_DATA_PAGE_SIZE = 1024 * 1024; // 1MB
  
  public final static long DEFAULT_SIDECAR_IO_BUFFER_SIZE = 1024 * 1024; // 1MB
  
  public final static int DEFAULT_SIDECAR_IO_POOL_SIZE = 32; // 1MB
  
  public final static boolean DEFAULT_SIDECAR_JMX_METRICS_ENABLED = true;
  
  public final static String DEFAULT_SIDECAR_JMX_METRICS_DOMAIN_NAME = "com.sidecar.metrics";
  
  public final static boolean DEFAULT_SIDECAR_TEST_MODE = false;
  
  public final static CacheType DEFAULT_DATA_CACHE_TYPE = CacheType.FILE;
  
  public final static boolean DEFAULT_SIDECAR_PERSISTENT_CACHE = true;
  
  
  private static SidecarConfig instance;
  
  private SidecarConfig() {
  }
  
  public synchronized static SidecarConfig fromHadoopConfiguration(Configuration conf) {
    SidecarConfig config = new SidecarConfig();
    Iterator<Map.Entry<String, String>> it = conf.iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> entry = it.next();
      String name = entry.getKey();
      if (isSidecarPropertyName(name)) {
        config.setProperty(name, entry.getValue());
      }
    }
    instance = config;
    return instance;
  }
  
  public synchronized static SidecarConfig getInstance() {
    if (instance == null) {
      instance = new SidecarConfig();
    }
    return instance;
  }
  
  public static String toCarrotPropertyName(String cacheName, String carrotName) {
    return cacheName + "." + carrotName;
  }
  
  private static boolean isSidecarPropertyName(String name) {
    return name.indexOf("sidecar") == 0; // starts with side car
  }
  
  /**
   * Is write cache enabled
   * @return write cache enabled
   */
  public boolean isWriteCacheEnabled() {
    String value = getProperty(SIDECAR_WRITE_CACHE_ENABLED_KEY);
    if (value != null) {
      return Boolean.valueOf(value);
    }
    return DEFAULT_SIDECAR_WRITE_CACHE_ENABLED;
  }
  
  /**
   * Set write cache enabled
   * @param b true or false
   */
  public SidecarConfig setWriteCacheEnabled(boolean b) {
    setProperty(SIDECAR_WRITE_CACHE_ENABLED_KEY, Boolean.toString(b));
    return this;
  }
  
  /**
   * Is test mode
   * @return true or false
   */
  public boolean isTestMode() {
    String value = getProperty(SIDECAR_TEST_MODE_KEY);
    if (value != null) {
      return Boolean.valueOf(value);
    }
    return DEFAULT_SIDECAR_TEST_MODE;
  }
  
  /**
   * Set test mode
   * @param b true or false
   */
  public SidecarConfig setTestMode(boolean b) {
    setProperty(SIDECAR_TEST_MODE_KEY, Boolean.toString(b));
    return this;
  }
  
  /**
   * Is JMX metrics enabled
   * @return true or false
   */
  public boolean isJMXMetricsEnabled() {
    String value = getProperty(SIDECAR_JMX_METRICS_ENABLED_KEY);
    if (value != null) {
      return Boolean.valueOf(value);
    }
    return DEFAULT_SIDECAR_JMX_METRICS_ENABLED;
  }
  
  /**
   * Set JMX metrics enabled
   * @param b true or false
   */
  public SidecarConfig setJMXMetricsEnabled(boolean b) {
    setProperty(SIDECAR_JMX_METRICS_ENABLED_KEY, Boolean.toString(b));
    return this;
  }
  
  /**
   * Get JMX metrics domain name
   * @return domain name
   */
  public String getJMXMetricsDomainName() {
    return getProperty(SIDECAR_JMX_METRICS_DOMAIN_NAME_KEY, 
      DEFAULT_SIDECAR_JMX_METRICS_DOMAIN_NAME);
  }
  
  /**
   * Set JMX metrics domain name
   * @param name domain name
   */
  public SidecarConfig setJMXMetricsDomainName(String name) {
    setProperty(SIDECAR_JMX_METRICS_DOMAIN_NAME_KEY, name);
    return this;
  }
  
  /**
   * Get write cache location as URI
   * @return location
   */
  public URI getWriteCacheURI() {
    String value = getProperty(SIDECAR_WRITE_CACHE_URI_KEY);
    if (value != null) {
      try {
        return new URI(value);
      } catch (URISyntaxException e) {
        LOG.error("getWriteCacheURI", e);
        return null;
      }
    }
    return null;
  }
  
  /**
   * Set write cache directory location
   * @param uri location
   */
  public SidecarConfig setWriteCacheURI(URI uri) {
    setProperty(SIDECAR_WRITE_CACHE_URI_KEY, uri.toString());
    return this;
  }
  
  /**
   * Get write cache size per instance
   * @return size
   */
  public long getWriteCacheSizePerInstance() {
    String value = getProperty(SIDECAR_WRITE_CACHE_SIZE_KEY);
    if (value != null) {
      return Long.parseLong(value);
    }
    return DEFAULT_SIDECAR_WRITE_CACHE_SIZE;
  }
  
  /**
   * Set write cache size per instance in bytes
   * @param size
   */
  public SidecarConfig setWriteCacheSizePerInstance(long size) {
    setProperty(SIDECAR_WRITE_CACHE_SIZE_KEY, Long.toString(size));
    return this;
  }
  
  /**
   * Get data page size 
   * @return size
   */
  public long getDataPageSize() {
    String value = getProperty(SIDECAR_DATA_PAGE_SIZE_KEY);
    if (value != null) {
      return Long.parseLong(value);
    }
    return DEFAULT_SIDECAR_DATA_PAGE_SIZE;
  }
  
  /**
   * Set data page size  in bytes
   * @param size data page size
   */
  public SidecarConfig setDataPageSize(long size) {
    setProperty(SIDECAR_DATA_PAGE_SIZE_KEY, Long.toString(size));
    return this;
  }
  
  /**
   * Get I/O buffer size in bytes 
   * @return size I/O buffer size
   */
  public long getIOBufferSize() {
    String value = getProperty(SIDECAR_IO_BUFFER_SIZE_KEY);
    if (value != null) {
      return Long.parseLong(value);
    }
    return DEFAULT_SIDECAR_IO_BUFFER_SIZE;
  }
  
  /**
   * Set I/O buffer size  in bytes
   * @param size I/O buffer size
   */
  public SidecarConfig setIOBufferSize(long size) {
    setProperty(SIDECAR_IO_BUFFER_SIZE_KEY, Long.toString(size));
    return this;
  }
  
  /**
   * Get I/O pool size
   * @return size I/O pool size
   */
  public int getIOPoolSize() {
    String value = getProperty(SIDECAR_IO_BUFFER_SIZE_KEY);
    if (value != null) {
      return Integer.parseInt(value);
    }
    return DEFAULT_SIDECAR_IO_POOL_SIZE;
  }
  
  /**
   * Set I/O pool size
   * @param size I/O pool size
   */
  public SidecarConfig setIOPoolSize(int size) {
    setProperty(SIDECAR_IO_POOL_SIZE_KEY, Integer.toString(size));
    return this;
  }
  
  /**
   * Get include path list for cache
   * @param cacheName cache name
   * @return list of include paths or null
   */
  public String[] getIncludePathList(String cacheName) {
    String value = getProperty(SIDECAR_INCLUDE_PATH_LIST_KEY);
    if (value != null) {
      String[] paths = value.split(",");
      // clean up
      Arrays.stream(paths).forEach( x -> x = x.trim());
      return paths;
    }
    return null; // no default value
  }
  
  /**
   * Set include path list for a cache 
   * @param cacheName cache name
   * @param list comma-separated  list of paths (regexes)
   * @return self
   */
  public SidecarConfig setIncludePathList(String cacheName, String list) {
    setProperty(SIDECAR_INCLUDE_PATH_LIST_KEY, list);
    return this;
  }
  
  /**
   * Get exclude path list for cache
   * @param cacheName cache name
   * @return list of include paths or null
   */
  public String[] getExcludePathList(String cacheName) {
    String value = getProperty(SIDECAR_INCLUDE_PATH_LIST_KEY);
    if (value != null) {
      String[] paths = value.split(",");
      // clean up
      Arrays.stream(paths).forEach(x -> x = x.trim());
      return paths;
    }
    return null; // no default value
  }
  
  /**
   * Set exclude path list for a cache 
   * @param cacheName cache name
   * @param list comma-separated list of paths (regexes)
   * @return self
   */
  public SidecarConfig setExcludePathList(String cacheName, String list) {
    setProperty(SIDECAR_INCLUDE_PATH_LIST_KEY, list);
    return this;
  }
  
  /** 
   * Get data cache type
   * @return data cache type
   */
  public CacheType getDataCacheType() {
    String value = getProperty(SIDECAR_DATA_CACHE_TYPE_KEY);
    if (value == null) {
      return DEFAULT_DATA_CACHE_TYPE;
    }
    return CacheType.valueOf(value.toUpperCase());
  }
  
  /**
   * Set data cache type
   * @param type data cache type
   * @return self
   */
  public SidecarConfig setDataCacheType(CacheType type) {
    setProperty(SIDECAR_DATA_CACHE_TYPE_KEY, type.getType());
    return this;
  }
  
  /**
   * Set cache persistent
   * @param b true or false
   * @return self
   */
  public SidecarConfig setPersistentCache(boolean b) {
    setProperty(SIDECAR_PERSISTENT_CACHE_KEY, Boolean.toString(b));
    return this;
  }
  
  /**
   * Is cache persistent
   * @return
   */
  public boolean isCachePersistent() {
    String value = getProperty(SIDECAR_PERSISTENT_CACHE_KEY);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return DEFAULT_SIDECAR_PERSISTENT_CACHE;
 
  }
}