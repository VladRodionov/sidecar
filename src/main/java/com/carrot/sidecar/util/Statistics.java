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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class Statistics {
    
    AtomicLong totalBytesRead = new AtomicLong();
    
    AtomicLong totalBytesReadRemote = new AtomicLong();
    
    AtomicLong totalBytesReadWriteCache = new AtomicLong();
    
    AtomicLong totalBytesReadDataCache = new AtomicLong();
    
    AtomicLong totalBytesReadPrefetch = new AtomicLong();
    
    AtomicLong totalReadRequests = new AtomicLong();
    
    AtomicLong totalReadRequestsFromWriteCache = new AtomicLong();
    
    AtomicLong totalReadRequestsFromDataCache = new AtomicLong();
    
    AtomicLong totalReadRequestsFromRemote = new AtomicLong();
    
    AtomicLong totalReadRequestsFromPrefetch = new AtomicLong();
    
    AtomicLong totalScansDetected = new AtomicLong();
    
    AtomicLong totalFilesCreated = new AtomicLong();
    
    AtomicLong totalFilesDeleted = new AtomicLong();
    
    AtomicLong totalFilesOpened = new AtomicLong();
    
    AtomicLong totalFilesOpenedInWriteCache = new AtomicLong();
    
    /**
     * Add total bytes read
     * @param n bytes
     * @return new value
     */
    public long addTotalBytesRead(long n) {
      return totalBytesRead.addAndGet(n);
    }
    
    /**
     * Get total bytes read
     * @return total bytes read
     */
    public long getTotalBytesRead() {
      return totalBytesRead.get();
    }
    
    /**
     * Add total bytes read from remote FS
     * @param n bytes
     * @return new value
     */
    public long addTotalBytesReadRemote(long n) {
      return totalBytesReadRemote.addAndGet(n);
    }
    
    /**
     * Get total bytes read from remote FS
     * @return total bytes read
     */
    public long getTotalBytesReadRemote() {
      return totalBytesReadRemote.get();
    }
    
    /**
     * Add total bytes read from data cache
     * @param n bytes
     * @return new value
     */
    public long addTotalBytesReadDataCache(long n) {
      return totalBytesReadDataCache.addAndGet(n);
    }
    
    /**
     * Get total bytes read from data cache
     * @return total bytes read
     */
    public long getTotalBytesReadDataCache() {
      return totalBytesReadDataCache.get();
    }
    
    /**
     * Add total bytes read from write cache
     * @param n bytes
     * @return new value
     */
    public long addTotalBytesReadWriteCache(long n) {
      return totalBytesReadWriteCache.addAndGet(n);
    }
    
    /**
     * Get total bytes read from write cache
     * @return total bytes read
     */
    public long getTotalBytesReadWriteCache() {
      return totalBytesReadWriteCache.get();
    }
    
    /**
     * Add total bytes read from prefetch
     * @param n bytes
     * @return new value
     */
    public long addTotalBytesReadPrefetch(long n) {
      return totalBytesReadPrefetch.addAndGet(n);
    }
    
    /**
     * Get total bytes read from prefetch
     * @return total bytes read
     */
    public long getTotalBytesReadPrefetch() {
      return totalBytesReadPrefetch.get();
    }
    
    /**
     * Add total read requests
     * @param n number
     * @return new value
     */
    public long addTotalReadRequests(long n) {
      return totalReadRequests.addAndGet(n);
    }
    
    /**
     * Get total read requests
     * @return total read requests
     */
    public long getTotalReadRequests() {
      return totalReadRequests.get();
    }
    
    /**
     * Add total read requests from remote FS
     * @param n number
     * @return new value
     */
    public long addTotalReadRequestsFromRemote(long n) {
      return totalReadRequestsFromRemote.addAndGet(n);
    }
    
    /**
     * Get total read requests from remote FS
     * @return total read requests from remote FS
     */
    public long getTotalReadRequestsFromRemote() {
      return totalReadRequestsFromRemote.get();
    }
    
    /**
     * Add total read requests from data cache
     * @param n number
     * @return new value
     */
    public long addTotalReadRequestsFromDataCache(long n) {
      return totalReadRequestsFromDataCache.addAndGet(n);
    }
    
    /**
     * Get total read read requests from data cache
     * @return total read requests from data cache
     */
    public long getTotalReadRequestsFromDataCache() {
      return totalReadRequestsFromDataCache.get();
    }
    
    /**
     * Add total read requests from write cache
     * @param n number
     * @return new value
     */
    public long addTotalReadRequestsFromWriteCache(long n) {
      return totalReadRequestsFromWriteCache.addAndGet(n);
    }
    
    /**
     * Get total read read requests from write cache
     * @return total read requests from write cache
     */
    public long getTotalReadRequestsFromWriteCache() {
      return totalReadRequestsFromWriteCache.get();
    }
    
    /**
     * Add total read requests from prefetch
     * @param n number
     * @return new value
     */
    public long addTotalReadRequestsFromPrefetch(long n) {
      return totalReadRequestsFromPrefetch.addAndGet(n);
    }
    
    /**
     * Get total read read requests from prefetch
     * @return total read requests from prefetch
     */
    public long getTotalReadRequestsFromPrefetch() {
      return totalReadRequestsFromPrefetch.get();
    }
    
    /**
     * Get total scan operation detected
     * @return total scans detected
     */
    public long getTotalScansDetected() {
      return this.totalScansDetected.get();
    }
    
    /**
     *  Add total scans detected
     * @param v number
     * @return new value
     */
    public long addTotalScansDetected(long v) {
      return this.totalScansDetected.addAndGet(v);
    }
    
    /**
     * Get total files created
     * @return total files created
     */
    public long getTotalFilesCreated() {
      return this.totalFilesCreated.get();
    }
    
    /**
     * Add total files created
     * @param n number of files
     * @return new value
     */
    public long addTotalFilesCreated(long n) {
      return this.totalFilesCreated.addAndGet(n);
    }
    
    /**
     * Get total files deleted
     * @return total files deleted
     */
    public long getTotalFilesDeleted() {
      return this.totalFilesDeleted.get();
    }
    
    /**
     * Add total files deleted
     * @param n number of files
     * @return new value
     */
    public long addTotalFilesDeleted(long n) {
      return this.totalFilesDeleted.addAndGet(n);
    }
    
    /**
     * Get total files opened
     * @return total files opened
     */
    public long getTotalFilesOpened() {
      return this.totalFilesOpened.get();
    }
    
    /**
     * Add total files opened
     * @param n number of files
     * @return new value
     */
    public long addTotalFilesOpened(long n) {
      return this.totalFilesOpened.addAndGet(n);
    }

    /**
     * Get total files opened in write cache
     * @return total files opened
     */
    public long getTotalFilesOpenedInWriteCache() {
      return this.totalFilesOpenedInWriteCache.get();
    }
    
    /**
     * Add total files opened in write cache
     * @param n number of files
     * @return new value
     */
    public long addTotalFilesOpenedInWriteCache(long n) {
      return this.totalFilesOpenedInWriteCache.addAndGet(n);
    }
    
    /**
     * Save statistics
     * @param dos data output stream
     * @throws IOException 
     */
    public void save(DataOutputStream dos) throws IOException {
       dos.writeLong(totalBytesRead.get()); 
       dos.writeLong(totalBytesReadRemote.get()); 
       dos.writeLong(totalBytesReadWriteCache.get());
       dos.writeLong(totalBytesReadDataCache.get());
       dos.writeLong(totalBytesReadPrefetch.get());
       dos.writeLong(totalReadRequests.get()); 
       dos.writeLong(totalReadRequestsFromWriteCache.get()); 
       dos.writeLong(totalReadRequestsFromDataCache.get()); 
       dos.writeLong(totalReadRequestsFromRemote.get()); 
       dos.writeLong(totalReadRequestsFromPrefetch.get());
       dos.writeLong(totalScansDetected.get());
       dos.writeLong(totalFilesCreated.get());
       dos.writeLong(totalFilesDeleted.get());
       dos.writeLong(totalFilesOpened.get());
       dos.writeLong(totalFilesOpenedInWriteCache.get());
       
    }
    
    /**
     * Load statistics
     * @param dis data input stream
     * @throws IOException 
     */
    public void load(DataInputStream dis) throws IOException {
      totalBytesRead.set(dis.readLong());
      totalBytesReadRemote.set(dis.readLong());
      totalBytesReadWriteCache.set(dis.readLong());
      totalBytesReadDataCache.set(dis.readLong());
      totalBytesReadPrefetch.set(dis.readLong());
      totalReadRequests.set(dis.readLong());
      totalReadRequestsFromWriteCache.set(dis.readLong());
      totalReadRequestsFromDataCache.set(dis.readLong());
      totalReadRequestsFromRemote.set(dis.readLong());
      totalReadRequestsFromPrefetch.set(dis.readLong());
      totalScansDetected.set(dis.readLong());
      totalFilesCreated.set(dis.readLong());
      totalFilesDeleted.set(dis.readLong());
      totalFilesOpened.set(dis.readLong());
      totalFilesOpenedInWriteCache.set(dis.readLong());
    }
    
    public void reset() {
      totalBytesRead.set(0);
      totalBytesReadRemote.set(0);
      totalBytesReadWriteCache.set(0);
      totalBytesReadDataCache.set(0);
      totalBytesReadPrefetch.set(0);
      totalReadRequests.set(0);
      totalReadRequestsFromWriteCache.set(0);
      totalReadRequestsFromDataCache.set(0);
      totalReadRequestsFromRemote.set(0);
      totalReadRequestsFromPrefetch.set(0);
      totalScansDetected.set(0);
      totalFilesCreated.set(0);
      totalFilesDeleted.set(0);
      totalFilesOpened.set(0);
      totalFilesOpenedInWriteCache.set(0);
    }
  }