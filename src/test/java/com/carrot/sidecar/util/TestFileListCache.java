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
package com.carrot.sidecar.util;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class TestFileListCache {
  int num = 10;
  LRUCache<String, Long> cache;
  
  @Before
  public void setUp() {
    cache = new LRUCache<>();
    for (int i = 0; i < num; i++) {
      cache.put("key" + i, (long) i);
    }
  }
  
  private void verify() {
    assertEquals(num , cache.size());
    for (int i = 0; i < num; i++) {
      String toEvict = cache.evictionCandidate();
      assertEquals("key" + i, toEvict);
      cache.remove(toEvict);
    }
  }
  
  @Test
  public void testCachePutGetRemove() {
    verify();
  }
  
  @Test
  public void testCacheSaveLoad() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    cache.save(baos);
    byte[] buf = baos.toByteArray();
    ByteArrayInputStream bais = new ByteArrayInputStream(buf);
    cache = new LRUCache<>();
    cache.load(bais);
    verify();
  }
  
}
