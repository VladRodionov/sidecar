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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseScanDetectorHint implements ScanDetectorHint {
  
  private static final Logger LOG = LoggerFactory.getLogger(HBaseScanDetectorHint.class);  

  
  private static Map<Thread, Boolean> nonCacheableThreads = 
      new ConcurrentHashMap<Thread, Boolean>();
   
  /** Stream access counter */
  private AtomicLong streamAccessCounter = new AtomicLong(0);
  
  public HBaseScanDetectorHint() {
  }
  
  @Override
  public boolean scanDetected() {
    Thread currentThread = Thread.currentThread();
    if (nonCacheableThreads.containsKey(currentThread)) {
      return true;
    }
    long count = streamAccessCounter.incrementAndGet();
    if (count % 10 == 0) {
      // This call is expensive: on my MBP Pro 2019 it takes 20 microseconds
      // So it is expensive to call it every time we read data from the stream
      // Therefore, we call it every 10th time
      StackTraceElement[] traces = currentThread.getStackTrace();
      String compactor = "org.apache.hadoop.hbase.regionserver.compactions.Compactor";
      for (int i = traces.length - 1; i >= 0; i--) {
        StackTraceElement e = traces[i];
        if (e.getClassName().equals(compactor)) {
          nonCacheableThreads.put(currentThread, Boolean.TRUE);
          return true;
        }
      }
    }
    return false;
  }
}
