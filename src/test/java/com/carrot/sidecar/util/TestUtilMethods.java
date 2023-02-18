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
package com.carrot.sidecar.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class TestUtilMethods {

  @Test
  public void testPatterns() {
    String list = ".*/oldWALs/.*,.*/archive/.*,.*/corrupt/.*,.*/staging/.*";
    String[] parts = list.split(",");
    Pattern[] patterns = new Pattern[parts.length];
    for (int i = 0; i < parts.length; i++) {
      patterns[i] = Pattern.compile(parts[i]);
    }
    
    String s1 = "/file/archive/etc"; // matches = true
    String s2 = "/file/staging/etc"; // matches = true
    String s3 = "/file/oldWALs/etc";
    String s4 = "/file/staging/etc";
    
    assertTrue(matches(s1, patterns));
    assertTrue(matches(s2, patterns));
    assertTrue(matches(s3, patterns));
    assertTrue(matches(s4, patterns));
    
    String s5 = "/file/archive1/etc"; // matches = true
    String s6 = "/file/staging1/etc"; // matches = true
    String s7 = "/file/oldWALs1/etc";
    String s8 = "/file/staging1/etc";
    
    assertFalse(matches(s5, patterns));
    assertFalse(matches(s6, patterns));
    assertFalse(matches(s7, patterns));
    assertFalse(matches(s8, patterns));
    
    String s = "hdfs://localhost:53048/user/vrodionov/test-data/d2386078-43e1-604a-2694-5a2452303006/data/default/TABLE_A/d7f21f69e6bfc0aee4b49e5177f69f42/.tmp/fam_a/79b44a583393499dba008a2e1d0d01a5";
    assertFalse(matches(s, patterns));
  }
  
  private boolean matches(String in, Pattern[] patterns) {
    for(Pattern p: patterns) {
      Matcher m = p.matcher(in);
      if (m.matches()) {
        return true;
      }
    }
    return false;
  }
}
