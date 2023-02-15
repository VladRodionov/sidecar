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
package com.carrot.sidecar.jmx;

import com.carrot.sidecar.SidecarCachingFileSystem;
import com.carrot.sidecar.SidecarCachingFileSystem.Statistics;

public class SidecarJMXSink implements SidecarJMXSinkMBean{

  private SidecarCachingFileSystem sidecar;
  
  public SidecarJMXSink(SidecarCachingFileSystem scfs) {
    this.sidecar = scfs;
  }
  
  @Override
  public String getremote_fs_uri() {
    return sidecar.getRemoteFSURI().toString();
  }

  @Override
  public boolean getremote_fs_files_mutable() {
    return sidecar.isMutableFS();
  }

  @Override
  public String getwrite_cache_mode() {
    return sidecar.getWriteCacheMode().toString();
  }

  @Override
  public boolean getmeta_cache_enabled() {
    return sidecar.isMetaCacheEnabled();
  }

  @Override
  public int getdata_page_size() {
    return sidecar.getDataPageSize();
  }

  @Override
  public int getdata_prefetch_buffer_size() {
    return sidecar.getPrefetchBufferSize();
  }


  @Override
  public long gettotal_bytes_read() {
    Statistics stats = sidecar.getStatistics();
    return stats.getTotalBytesRead();
  }

  @Override
  public long gettotal_bytes_read_remote_fs() {
    Statistics stats = sidecar.getStatistics();
    return stats.getTotalBytesReadRemote();
  }

  @Override
  public long gettotal_bytes_read_data_cache() {
    Statistics stats = sidecar.getStatistics();
    return stats.getTotalBytesReadDataCache();
  }

  @Override
  public long gettotal_bytes_read_write_cache() {
    Statistics stats = sidecar.getStatistics();
    return stats.getTotalBytesReadWriteCache();
  }

  @Override
  public long gettotal_bytes_read_prefetch() {
    Statistics stats = sidecar.getStatistics();
    return stats.getTotalBytesReadPrefetch();
  }
  
  @Override
  public long gettotal_read_requests() {
    Statistics stats = sidecar.getStatistics();
    return stats.getTotalReadRequests();
  }

  @Override
  public long gettotal_read_requests_remote_fs() {
    Statistics stats = sidecar.getStatistics();
    return stats.getTotalReadRequestsFromRemote();
  }

  @Override
  public long gettotal_read_requests_write_cache() {
    Statistics stats = sidecar.getStatistics();
    return stats.getTotalReadRequestsFromWriteCache();
  }

  @Override
  public long gettotal_read_requests_data_cache() {
    Statistics stats = sidecar.getStatistics();
    return stats.getTotalReadRequestsFromDataCache();
  }
  
  @Override
  public long gettotal_read_requests_prefetch() {
    Statistics stats = sidecar.getStatistics();
    return stats.getTotalReadRequestsFromPrefetch();
  }
  
  @Override
  public long gettotal_scans_detected() {
    Statistics stats = sidecar.getStatistics();
    return stats.getTotalScansDetected();
  }
  
  @Override
  public long gettotal_files_created() {
    Statistics stats = sidecar.getStatistics();
    return stats.getTotalFilesCreated();
  }
  
  @Override
  public long gettotal_files_deleted() {
    Statistics stats = sidecar.getStatistics();
    return stats.getTotalFilesDeleted();
  }
  
  @Override
  public long gettotal_files_opened() {
    Statistics stats = sidecar.getStatistics();
    return stats.getTotalFilesOpened();
  }
  
  @Override
  public long gettotal_files_opened_write_cache() {
    Statistics stats = sidecar.getStatistics();
    return stats.getTotalFilesOpenedInWriteCache();
  }
}
