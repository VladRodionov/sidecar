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
package com.carrot.sidecar.hints;

import com.carrot.sidecar.SidecarConfig;

public interface CacheOnReadHint {
  /**
   * Check current thread stack trace and determine
   * if current session is cacheable
   * @return true - if yes, false - otherwise
   */
  public boolean cacheOnReadThread();
  
  /**
   * Get instance from configuration 
   * @param config sidecar configuration
   * @return instance of hint class
   */
  public static CacheOnReadHint fromConfig(SidecarConfig config) {
    return config.getCacheOnReadHint();
  }
}
