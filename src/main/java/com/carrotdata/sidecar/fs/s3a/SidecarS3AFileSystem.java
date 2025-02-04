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
package com.carrotdata.sidecar.fs.s3a;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.util.Progressable;

import com.carrotdata.sidecar.MetaDataCacheable;
import com.carrotdata.sidecar.RemoteFileSystemAccess;
import com.carrotdata.sidecar.SidecarCachingFileSystem;

/**
 * Sidecar caching File System for Amazon s3 Object Store
 * fs.s3a.impl=com.carrotdata.sidecar.s3a.SidecarS3AFileSystem
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SidecarS3AFileSystem extends S3AFileSystem 
  implements MetaDataCacheable, RemoteFileSystemAccess {

  private SidecarCachingFileSystem sidecar;
  
  public SidecarS3AFileSystem() {}
  
  /**
   * File System API
   */

  @Override
  public FileStatus getFileStatus(Path p) throws IOException {
    return sidecar.getFileStatus(p);
  }
  @Override
  public void initialize(URI name, Configuration originalConf) throws IOException {
    super.initialize(name, originalConf);
    this.sidecar = SidecarCachingFileSystem.get(this);
    this.sidecar.initialize(name, originalConf);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return sidecar.open(f, bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    return sidecar.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path path, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return sidecar.createNonRecursive(path, permission, flags, bufferSize, replication, blockSize,
      progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    return sidecar.append(f, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return sidecar.rename(src, dst);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return sidecar.delete(f, recursive);
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission)
      throws IOException, FileAlreadyExistsException {
    return sidecar.mkdirs(path, permission);
  }
  
  @Override
  public void close() throws IOException {
    super.close();
    sidecar.close();
  }

  @Override
  public SidecarCachingFileSystem getCachingFileSystem() {
    return sidecar;
  }
  @Override
  public FSDataInputStream openRemote(Path f, int bufferSize) throws IOException {
    return super.open(f, bufferSize);
  }

  @Override
  public FSDataOutputStream createRemote(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    return super.create(f, permission, overwrite,
      bufferSize, replication, blockSize, progress) ;
  }

  @Override
  public FSDataOutputStream createNonRecursiveRemote(Path path, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return super.createNonRecursive(path, permission, flags, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream appendRemote(Path f, int bufferSize, Progressable progress)
      throws IOException {
    return super.append(f, bufferSize, progress);
  }

  @Override
  public boolean renameRemote(Path src, Path dst) throws IOException {
    return super.rename(src, dst);
  }
  
  @SuppressWarnings("deprecation")
  @Override
  public void renameRemote(Path src, Path dst, Rename... options) throws IOException
  {
    super.rename(src, dst, options);
  }

  @Override
  public boolean deleteRemote(Path f, boolean recursive) throws IOException {
    return super.delete(f, recursive);
  }

  @Override
  public boolean mkdirsRemote(Path path, FsPermission permission)
      throws IOException, FileAlreadyExistsException {
    return super.mkdirs(path, permission);
  }
  @Override
  public FSDataOutputStream createNonRecursiveRemote(Path path, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
      throws IOException {
    return super.createNonRecursive(path, overwrite, bufferSize, replication, blockSize, progress);
  }
  
  @Override
  public FileStatus getFileStatusRemote(Path p) throws IOException {
    return super.getFileStatus(p);
  }
  
  @Override
  public FSDataOutputStream createRemote(Path f, FsPermission permission,
      EnumSet<CreateFlag> cflags, int bufferSize, short replication, long blockSize,
      Progressable progress, ChecksumOpt checksumOpt) throws IOException{
    return super.create(f, permission, cflags, bufferSize, replication, blockSize, progress, checksumOpt);
  }
}
