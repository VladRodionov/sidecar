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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SidecarConfig extends Properties{

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(SidecarConfig.class);
  
  public final static String SIDECAR_WRITE_CACHE_ENABLED_KEY = "sidecar.write.cache.enabled";
  
  public final static String SIDECAR_WRITE_CACHE_URI_KEY = "sidecar.write.cache.uri";
  
  /** This is not a global size, but per server instance */
  public final static String SIDECAR_WRITE_CACHE_SIZE_KEY = "sidecar.write.cache.size";
  
  public final static String SIDECAR_DATA_PAGE_SIZE_KEY = "sidecar.data.page.size";
  
  public final static String SIDECAR_IO_BUFFER_SIZE_KEY = "sidecar.io.buffer.size";
  
  public final static String SIDECAR_IO_POOL_SIZE_KEY = "sidecar.io.pool.size";
  
  public final static String SIDECAR_JMX_METRICS_ENABLED_KEY ="sidecar.jmx.metrics.enabled";
  
  public final static String SIDECAR_JMX_METRICS_DOMAIN_NAME_KEY ="sidecar.jmx.metrics.domain.name";
  
  public final static boolean DEFAULT_SIDECAR_WRITE_CACHE_ENABLED = false;
  
  public final static long DEFAULT_SIDECAR_WRITE_CACHE_SIZE = 0;
  
  public final static long DEFAULT_SIDECAR_DATA_PAGE_SIZE = 1024 * 1024; // 1MB
  
  public final static long DEFAULT_SIDECAR_IO_BUFFER_SIZE = 1024 * 1024; // 1MB
  
  public final static int DEFAULT_SIDECAR_IO_POOL_SIZE = 32; // 1MB
  
  public final static boolean DEFAULT_SIDECAR_JMX_METRICS_ENABLED = true;
  
  public final static String DEFAULT_SIDECAR_JMX_METRICS_DOMAIN_NAME = "com.sidecar.metrics";
  
  
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
  
  private static boolean isSidecarPropertyName(String name) {
    return name.indexOf("sidecar") >= 0;
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
  public void setWriteCacheEnabled(boolean b) {
    setProperty(SIDECAR_WRITE_CACHE_ENABLED_KEY, Boolean.toString(b));
    
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
  public void setJMXMetricsEnabled(boolean b) {
    setProperty(SIDECAR_JMX_METRICS_ENABLED_KEY, Boolean.toString(b));
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
  public void setJMXMetricsDomainName(String name) {
    setProperty(SIDECAR_JMX_METRICS_DOMAIN_NAME_KEY, name);
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
  public void setWriteCacheURI(URI uri) {
    setProperty(SIDECAR_WRITE_CACHE_URI_KEY, uri.toString());
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
  public void setWriteCacheSizePerInstance(long size) {
    setProperty(SIDECAR_WRITE_CACHE_SIZE_KEY, Long.toString(size));
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
  public void setDataPageSize(long size) {
    setProperty(SIDECAR_DATA_PAGE_SIZE_KEY, Long.toString(size));
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
  public void setIOBufferSize(long size) {
    setProperty(SIDECAR_IO_BUFFER_SIZE_KEY, Long.toString(size));
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
  public void setIOPoolSize(int size) {
    setProperty(SIDECAR_IO_POOL_SIZE_KEY, Integer.toString(size));
  }
  
}
