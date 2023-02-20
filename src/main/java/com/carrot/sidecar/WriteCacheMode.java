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

public enum WriteCacheMode {
  DISABLED("DISABLED"),
  SYNC ("SYNC"),       // for caching FS which is non-distributed and not HA - this is the only mode
  ASYNC_CLOSE ("ASYNC_CLOSE"),      // for distributed and HA (redundant) 
  ASYNC_COPY ("ASYNC_COPY");
  
  private final String mode;
  
  WriteCacheMode(String mode) {
    this.mode = mode;
  }
  
  public String getMode() {
    return mode;
  }
  
  public static WriteCacheMode defaultMode() {
    return WriteCacheMode.DISABLED;
  }
}
