/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.carrotdata.sidecar.jmx;

public interface SidecarSiteJMXSinkMBean {

  String getwrite_cache_uri();
  
  long getwritecache_max_size();

  long getwrite_cache_current_size();

  double getwrite_cache_used_ratio();

  long getwrite_cache_number_files();
  
  long getwrite_cache_bytes_written();

  int getpending_io_tasks();
  
  long getdata_set_size_disk();

}
