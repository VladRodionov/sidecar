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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrot.sidecar.RemoteFileSystemAccess;
import com.carrot.sidecar.SidecarCachingFileSystem;

public class CachedFileStatus extends FileStatus {
  
  private static final Logger LOG = LoggerFactory.getLogger(SidecarCachingFileSystem.class);  

  /**
   * Source file status
   */
  private FileStatus source;
  /**
   * File System for the file
   */
  private RemoteFileSystemAccess remoteFs;
  
  /**
   * Constructor
   * @param fs file system
   * @param p path to the file
   * @param modificationTime file modification time
   * @param length file length
   * @param isDir - always false (for future)
   */
  public CachedFileStatus(FileSystem fs, Path p, long modificationTime, long length, boolean isDir) {
    super(length, isDir, 0, 0L, modificationTime, p);
    this.remoteFs = (RemoteFileSystemAccess) fs;
  }

  @Override
  public long getLen() {
    if (this.source != null) {
      return this.source.getLen();
    }
    return super.getLen();
  }

  @Override
  public boolean isFile() {
    if (this.source != null) {
      return this.source.isFile();
    }
    return super.isFile();
  }

  @Override
  public boolean isDirectory(){
    if (this.source != null) {
      return this.source.isDirectory();
    }
    return super.isDirectory();
  }

  @Override
  public boolean isDir() {
    if (this.source != null) {
      return this.source.isDirectory();
    }
    return super.isDirectory();
  }
  
  @Override
  public long getModificationTime() {
    if (this.source != null) {
      return this.source.getModificationTime();
    }
    return super.getModificationTime();
  }
  
  @Override
  public Path getPath() {
    return super.getPath();
  }
  
  /**
   * These API requires loading original FileStatus 
   */
  
  private synchronized void checkRemoteLoaded() {
    
    if (this.source != null) {
      return;
    }
    
    Path p = getPath();
    try {
      this.source = remoteFs.getFileStatusRemote(p);
    } catch (IOException e) {
      LOG.error("CachedFileStatus: failed to get remote file status", e);
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public boolean isSymlink() {
    checkRemoteLoaded();
    return this.source.isSymlink();
  }

  @Override
  public long getBlockSize() {
    checkRemoteLoaded();
    return this.source.getBlockSize();
  }

  @Override
  public short getReplication() {
    checkRemoteLoaded();
    return this.source.getReplication();
  }

  @Override
  public long getAccessTime() {
    checkRemoteLoaded();
    return this.source.getAccessTime();
  }

  @Override
  public FsPermission getPermission() {
    checkRemoteLoaded();
    return this.source.getPermission();
  }

  @Override
  public boolean isEncrypted() {
    checkRemoteLoaded();
    return this.source.isEncrypted();
  }

  @Override
  public String getOwner() {
    checkRemoteLoaded();
    return this.source.getOwner();
  }

  @Override
  public String getGroup() {
    checkRemoteLoaded();
    return this.source.getGroup();
  }

  @Override
  public Path getSymlink() throws IOException {
    checkRemoteLoaded();
    return this.source.getSymlink();
  }
 
  @Override
  public void write(DataOutput out) throws IOException {
    checkRemoteLoaded();
    this.source.write(out);
  }

  @Override
  public int compareTo(FileStatus o) {
    checkRemoteLoaded();
    return this.source.compareTo(o);
  }

  @Override
  public boolean equals(Object o) {
    checkRemoteLoaded();
    return this.source.equals(o);
  }

  @Override
  public String toString() {
    checkRemoteLoaded();
    return this.source.toString();
  }
}
