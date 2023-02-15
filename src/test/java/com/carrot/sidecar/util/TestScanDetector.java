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

import static org.junit.Assert.*;

import org.junit.Test;

public class TestScanDetector {
  
  @Test
  public void testScanDetector() {
    ScanDetector sd = new ScanDetector(10, 100);
    // No scan
    for (int i = 0; i < 200; i++) {
      boolean result = sd.record(i * 101);
      assertFalse(result);
    }
    sd.reset();
    // Scan first 100
    for (int i = 0; i < 200; i++) {
      boolean result = sd.record(i * 100);
      if (i < 9) {
        assertFalse(result);
      } else {
        assertTrue(result);
      }
    }
    sd.reset();
    sd.record(1);
    sd.record(99);
    sd.record(1);
    sd.record(99);
    sd.record(101);
    // Scan after first 5;
    for (int i = 0; i < 200; i++) {
      boolean result = sd.record(i * 100);
      if (i < 9) {
        assertFalse(result);
      } else {
        assertTrue(result);
      }
    } 
  }
}
