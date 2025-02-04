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

import org.apache.hadoop.conf.Configuration;

import com.carrotdata.cache.controllers.AQBasedAdmissionController;
import com.carrotdata.cache.controllers.MinAliveRecyclingSelector;
import com.carrotdata.cache.eviction.SLRUEvictionPolicy;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.sidecar.SidecarConfig;
import com.carrotdata.sidecar.SidecarDataCacheType;

public class TestCachingFileSystemHybrid extends TestCachingFileSystemBase{

  
  protected SidecarDataCacheType cacheType = SidecarDataCacheType.HYBRID;

  @Override
  protected Configuration getConfiguration() {
    SidecarConfig cacheConfig = SidecarConfig.getInstance();
    cacheConfig
      .setDataPageSize(pageSize)
      .setIOBufferSize(ioBufferSize)
      .setDataCacheType(SidecarDataCacheType.HYBRID)
      .setJMXMetricsEnabled(true);
    
    CacheConfig carrotCacheConfig = CacheConfig.getInstance();
    
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.DATA_CACHE_OFFHEAP_NAME, offheapCacheSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.DATA_CACHE_OFFHEAP_NAME, offheapCacheSegmentSize);
    carrotCacheConfig.setCacheEvictionPolicy(SidecarConfig.DATA_CACHE_OFFHEAP_NAME, SLRUEvictionPolicy.class.getName());
    carrotCacheConfig.setRecyclingSelector(SidecarConfig.DATA_CACHE_OFFHEAP_NAME, MinAliveRecyclingSelector.class.getName());
    carrotCacheConfig.setSLRUInsertionPoint(SidecarConfig.DATA_CACHE_OFFHEAP_NAME, 6);
    if (aqEnabledOffheap) {
      carrotCacheConfig.setAdmissionController(SidecarConfig.DATA_CACHE_OFFHEAP_NAME, AQBasedAdmissionController.class.getName());
      carrotCacheConfig.setAdmissionQueueStartSizeRatio(SidecarConfig.DATA_CACHE_OFFHEAP_NAME, aqStartRatio);
    }
    
    if (scavThreads > 1) {
      carrotCacheConfig.setScavengerNumberOfThreads(SidecarConfig.DATA_CACHE_OFFHEAP_NAME, scavThreads);      
    }
    
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.DATA_CACHE_FILE_NAME, fileCacheSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.DATA_CACHE_FILE_NAME, fileCacheSegmentSize);
    carrotCacheConfig.setCacheEvictionPolicy(SidecarConfig.DATA_CACHE_FILE_NAME, SLRUEvictionPolicy.class.getName());
    carrotCacheConfig.setRecyclingSelector(SidecarConfig.DATA_CACHE_FILE_NAME, MinAliveRecyclingSelector.class.getName());
    carrotCacheConfig.setSLRUInsertionPoint(SidecarConfig.DATA_CACHE_FILE_NAME, 6);
    if (aqEnabledFile) {
      carrotCacheConfig.setAdmissionController(SidecarConfig.DATA_CACHE_FILE_NAME, AQBasedAdmissionController.class.getName());
      carrotCacheConfig.setAdmissionQueueStartSizeRatio(SidecarConfig.DATA_CACHE_FILE_NAME, aqStartRatio);
    }
    
    if (scavThreads > 1) {
      carrotCacheConfig.setScavengerNumberOfThreads(SidecarConfig.DATA_CACHE_FILE_NAME, scavThreads);      
    }
    
    carrotCacheConfig.setVictimCachePromotionOnHit(SidecarConfig.DATA_CACHE_FILE_NAME, victim_promoteOnHit);
    carrotCacheConfig.setVictimPromotionThreshold(SidecarConfig.DATA_CACHE_FILE_NAME, victim_promoteThreshold);
    
    Configuration configuration = TestUtils.getHdfsConfiguration(cacheConfig, carrotCacheConfig);
    return configuration;
  }

}
