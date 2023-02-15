/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.util.Arrays;

public class ScanDetector {

  private long records[];
  private int current;
  private int pageSize;
  
  public ScanDetector(int scanThreshold, int pageSize) {
    this.records = new long[scanThreshold];
    this.pageSize = pageSize;
    this.current = 0;
  }
  
  /**
   * Get scan threshold
   * @return
   */
  public int getScanThreshold() {
    return this.records.length;
  }
  
  /**
   * Record next access location
   * @param v access offset
   * @return true if scan detected
   */
  public synchronized boolean record(long v) {

    records[current] = v;
    current = (current + 1) % records.length;
    boolean scanDetected = true;
    for (int i = 0; i < records.length - 1; i++) {
      int cur = (current + i) % records.length;
      int next = (current + i + 1) % records.length;
      if (records[next] - records[cur] != pageSize) {
        scanDetected = false;
        break;
      }
    }
    return scanDetected;
  }
  
  public synchronized long current() {
    return this.records[current];
  }
  
  /**
   * Reset scan detector
   */
  public void reset () {
    this.current = 0;
    Arrays.setAll(records, x -> 0);
  }
}
