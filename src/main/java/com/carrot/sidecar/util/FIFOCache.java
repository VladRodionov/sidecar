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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class FIFOCache<K, V> {
  public static String NAME = "fifo-cache";
  public static String FILE_NAME = "fifo.cache";
  private static final int INIT_CAPACITY = 2000;
  private static final float INIT_LOAD_FACTOR = 0.75f;
  private static final boolean ACCESS_ORDERED = false;

  protected Map<K, V> mFIFOCache =
      Collections.synchronizedMap(new LinkedHashMap<>(INIT_CAPACITY,
          INIT_LOAD_FACTOR, ACCESS_ORDERED));
  
  public FIFOCache() {}
  
  /**
   * Put key value
   * @param key key object
   * @param value value object
   * @return previous value
   */
  public V put(K key, V value) {
    return mFIFOCache.put(key, value);
  }
  
  /**
   * Get value by key
   * @param key key object
   * @return value object
   */
  public V get(K key) {
    return mFIFOCache.get(key);
  }
  
  /**
   * Remove object
   * @param key key 
   * @return value
   */
  public V remove(K key) {
    return mFIFOCache.remove(key);
  }
  
  /**
   * Get eviction candidate
   * @return eviction candidate
   */
  public K evictionCandidate() {
    return mFIFOCache.keySet().iterator().next();
  }
  
  /**
   * Get size of a cache
   * @return size
   */
  public int size() {
    return mFIFOCache.size();
  }
  
  public void save(OutputStream os) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(os);
    oos.writeObject(mFIFOCache);
    oos.close();
  }
  
  @SuppressWarnings("unchecked")
  public void load(InputStream is) throws IOException{
    ObjectInputStream ois = new ObjectInputStream(is);
    try {
      this.mFIFOCache = (Map<K, V>) ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }
}
