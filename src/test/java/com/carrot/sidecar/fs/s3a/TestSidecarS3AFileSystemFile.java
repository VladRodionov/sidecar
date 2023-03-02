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
package com.carrot.sidecar.fs.s3a;

import org.apache.hadoop.conf.Configuration;

import com.carrot.cache.controllers.AQBasedAdmissionController;
import com.carrot.cache.controllers.MinAliveRecyclingSelector;
import com.carrot.cache.eviction.SLRUEvictionPolicy;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.sidecar.SidecarConfig;
import com.carrot.sidecar.SidecarDataCacheType;
import com.carrot.sidecar.TestUtils;
import com.carrot.sidecar.WriteCacheMode;

public class TestSidecarS3AFileSystemFile  extends TestSidecarS3AFileSystemBase{
  
  @Override
  protected Configuration getConfiguration() {
    SidecarConfig cacheConfig = SidecarConfig.getInstance();
    cacheConfig
      .setDataPageSize(pageSize)
      .setIOBufferSize(ioBufferSize)
      .setDataCacheType(SidecarDataCacheType.FILE)
      .setJMXMetricsEnabled(false)
      .setWriteCacheMode(useWriteCache? WriteCacheMode.SYNC: WriteCacheMode.DISABLED)
      .setTestMode(true); // do not install shutdown hooks
    if (useWriteCache) {
      cacheConfig.setWriteCacheSizePerInstance(writeCacheMaxSize);
      cacheConfig.setWriteCacheURI(writeCacheDirectory);
    }
    
    CarrotConfig carrotCacheConfig = CarrotConfig.getInstance();
    
    carrotCacheConfig.setGlobalCacheRootDir(cacheDirectory.getPath());
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.DATA_CACHE_FILE_NAME, fileCacheSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.DATA_CACHE_FILE_NAME, fileCacheSegmentSize);
    carrotCacheConfig.setCacheEvictionPolicy(SidecarConfig.DATA_CACHE_FILE_NAME, SLRUEvictionPolicy.class.getName());
    carrotCacheConfig.setRecyclingSelector(SidecarConfig.DATA_CACHE_FILE_NAME, MinAliveRecyclingSelector.class.getName());
    carrotCacheConfig.setSLRUInsertionPoint(SidecarConfig.DATA_CACHE_FILE_NAME, 6);
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.META_CACHE_NAME, metaCacheSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.META_CACHE_NAME, metaCacheSegmentSize);

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
