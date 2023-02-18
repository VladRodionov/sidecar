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
package com.carrot.sidecar.fs.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.EnumSet;

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
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrot.sidecar.MetaDataCacheable;
import com.carrot.sidecar.RemoteFileSystemAccess;
import com.carrot.sidecar.SidecarCachingFileSystem;

/**
 * Sidecar caching File System for Hadoop HDFS
 * fs.hdfs.impl=com.carrot.sidecar.hdfs.SidecarDistributedFileSystem
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SidecarDistributedFileSystem extends DistributedFileSystem implements 
  MetaDataCacheable, RemoteFileSystemAccess {
  private static final Logger LOG = LoggerFactory.getLogger(SidecarDistributedFileSystem.class);  

  private SidecarCachingFileSystem sidecar;
  
  public SidecarDistributedFileSystem() {}
  
  @Override
  public void initialize(URI name, Configuration originalConf) throws IOException {
    super.initialize(name, originalConf);
    this.sidecar = SidecarCachingFileSystem.get(this);
    //Can we use single instance per process?
    this.sidecar.initialize(name, originalConf);
  }

  @Override
  public FileStatus getFileStatus(Path p) throws IOException {
    return sidecar.getFileStatus(p);
  }
  
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return sidecar.open(f, bufferSize);
  }

  @Override
  public FSDataOutputStream append(Path f, EnumSet<CreateFlag> flag, int bufferSize,
      Progressable progress) throws IOException {
    LOG.error("***Sidecar APPEND(Path f, EnumSet<CreateFlag> flag, int bufferSize,\n"
        + "      Progressable progress)");
    return super.append(f, flag, bufferSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, EnumSet<CreateFlag> flag, int bufferSize,
      Progressable progress, InetSocketAddress[] favoredNodes) throws IOException {
    LOG.error("***Sidecar APPEND(Path f, EnumSet<CreateFlag> flag, int bufferSize,\n"
        + "      Progressable progress, InetSocketAddress[] favoredNodes)");
    return super.append(f, flag, bufferSize, progress, favoredNodes);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> cflags,
      int bufferSize, short replication, long blockSize, Progressable progress,
      ChecksumOpt checksumOpt) throws IOException {
    return sidecar.create(f, permission, cflags, bufferSize, replication, blockSize, progress,
      checksumOpt);
  }

  @Override
  public HdfsDataOutputStreamBuilder createFile(Path path) {
    LOG.error("***Sidecar CREATEFILE(Path path)");
    return super.createFile(path);
  }

  @Override
  public HdfsDataOutputStreamBuilder appendFile(Path path) {
    LOG.error("***Sidecar APPENDFILE(Path path)");
    return super.appendFile(path);
  }

  @Override
  public HdfsDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress,
      InetSocketAddress[] favoredNodes) throws IOException {
    LOG.error("***Sidecar HdfsDataOutputStream CREATE(Path f, FsPermission permission, boolean overwrite,\n"
        + "      int bufferSize, short replication, long blockSize, Progressable progress,\n"
        + "      InetSocketAddress[] favoredNodes)");
    return super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress,
      favoredNodes);
  }

  @Override
  public void concat(Path trg, Path[] psrcs) throws IOException {
    LOG.error("***Sidecar CONCAT(Path trg, Path[] psrcs)");
    super.concat(trg, psrcs);
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
  public void rename(Path src, Path dst, Rename... options) throws IOException {
    sidecar.rename(src, dst, options);
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

  /**
   *  RemoteFileSystemAccess API
   */
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
  
  @Override
  public void renameRemote(Path src, Path dst, Rename... options) throws IOException {
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
