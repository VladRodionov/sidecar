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
package com.carrotdata.sidecar.hints;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class HBaseCachingHintDetector implements CachingHintDetector {
  
  
  private static Map<Thread, Boolean> nonCacheableThreads = 
      new ConcurrentHashMap<Thread, Boolean>();
   
  /** Stream access counter */
  private AtomicLong streamAccessCounter = new AtomicLong(0);
  
  public HBaseCachingHintDetector() {
  }
  
  @Override
  public boolean doNotCache(boolean read) {
    // Default implementation check presence of compaction in both: read and write path
    Thread currentThread = Thread.currentThread();
    if (nonCacheableThreads.containsKey(currentThread)) {
      return true;
    }
    long count = streamAccessCounter.getAndIncrement();
    if (count % 10 == 0) {
      // This call is expensive: on my MBP Pro 2019 it takes 20 microseconds
      // So it is expensive to call it every time we read data from the stream
      // Therefore, we call it every 10th time
      StackTraceElement[] traces = currentThread.getStackTrace();
      String compactor = "org.apache.hadoop.hbase.regionserver.compactions.Compactor";
      String flusher = "org.apache.hadoop.hbase.regionserver.StoreFlusher";
      String lookUp = read? compactor: flusher;
      for (int i = traces.length - 1; i >= 0; i--) {
        StackTraceElement e = traces[i];
        if (e.getClassName().equals(lookUp)) {
          if (read) {
            nonCacheableThreads.put(currentThread, Boolean.TRUE);
            return true;
          } else {
            return false;
          }
        }
      }
    }
    return read? false: true;
  }
}
