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
package com.carrotdata.sidecar.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Priority Queue, where priority is size of an object  
 *
 */
public class SizeBasedPriorityQueue {
  
  static class Pair implements Serializable {
    private static final long serialVersionUID = 4409861195414422007L;
    String path;
    long size;
    Pair(String path, long size){
      this.path = path;
      this.size = size;
    }
    
    @Override
    public boolean equals(Object o) {
      if (o instanceof Pair) {
        Pair p = (Pair)o;
        return path.equals(p.path);
      } else {
        return false;
      }
    }
  }
  
  public static String NAME = "priority-queue";
  public static String FILE_NAME = "write-cache-file-list.cache";
  
  private PriorityQueue<Pair> queue = new PriorityQueue<>(100, (x, y) -> {
    if (x.size > y.size) {
      return -1;
    } else if (x.size < y.size) {
      return 1;
    }
    return 0;
  });
  
  public SizeBasedPriorityQueue() {}
  
  /**
   * Add pair 
   * @param path path
   * @param size size
   */
  public synchronized void put(String path, long size) {
     queue.add(new Pair(path, size));
  }
  
  /**
   * Get value by key
   * @param key key object
   * @return value object
   */
  public synchronized Long get(String key) {
    Iterator<Pair> it = queue.iterator();
    while(it.hasNext()) {
      Pair p = it.next();
      if (key.equals(p.path)) {
        return Long.valueOf(p.size);
      }
    }
    return null;
  }
  
  /**
   * Remove object
   * @param key key 
   * @return value
   */
  public synchronized Long remove(String p) {
    Long size = get(p);
    if (size != null) {
      queue.remove(new Pair(p, 0));
    }
    return size;
  }
  
  /**
   * Exists key
   * @param key
   * @return true or false
   */
  public synchronized boolean exists(String p) {
    return get(p) != null;
  }
  
  /**
   * Get eviction candidate
   * @return eviction candidate
   */
  public synchronized String evictionCandidate() {
    Pair p = queue.peek();
    if (p != null) {
      return p.path;
    }
    return null;
  }
  
  /**
   * Get size of a cache
   * @return size
   */
  public synchronized int size() {
    return queue.size();
  }
  
  public void save(OutputStream os) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(os);
    oos.writeObject(queue);
    oos.close();
  }
  
  @SuppressWarnings("unchecked")
  public void load(InputStream is) throws IOException{
    ObjectInputStream ois = new ObjectInputStream(is);
    try {
      this.queue = (PriorityQueue<Pair>) ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }
  
  public static void main(String[] args) {
    SizeBasedPriorityQueue q = new SizeBasedPriorityQueue();
    
    q.put("1", 10000);
    q.put("3", 100000);
    q.put("2", 10001);
    
    System.out.printf("first=%s second=%s third=%s", 
      q.evictionCandidate(), q.evictionCandidate(), q.evictionCandidate());
  }
}
