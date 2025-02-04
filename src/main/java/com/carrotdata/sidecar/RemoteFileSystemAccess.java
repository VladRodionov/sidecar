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
package com.carrotdata.sidecar;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public interface RemoteFileSystemAccess {

  /**
   * Get caching file system
   * @return caching file system
   */
  public SidecarCachingFileSystem getCachingFileSystem();

  /**
   * Get file status from remote FS
   * @param p file path
   * @return file status
   * @throws IOException
   */
  public FileStatus getFileStatusRemote(Path p) throws IOException;
  /**
   * Concatenate remote files
   * @param trg target file
   * @param pathes files to concatenate
   * @throws IOException
   */
  public default void concatRemote(Path trg, Path[] pathes) throws IOException{}
  
  /**
   * Open remote path
   * @param f path
   * @param bufferSize buffer size
   * @return input stream
   * @throws IOException
   */
  public FSDataInputStream openRemote(Path f, int bufferSize) throws IOException;

  /**
   * Create remote file
   * @param f path
   * @param permission file permission
   * @param overwrite overwrite file
   * @param bufferSize buffer size
   * @param replication replication factor
   * @param blockSize block size
   * @param progress progress listener
   * @return output stream
   * @throws IOException
   */
  public FSDataOutputStream createRemote(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException;

  /**
   * Create remote file (non-recursive)
   * @param f path
   * @param permission file permission
   * @param flags flags
   * @param overwrite overwrite file
   * @param bufferSize buffer size
   * @param replication replication factor
   * @param blockSize block size
   * @param progress progress listener
   * @return output stream
   * @throws IOException
   */
  public FSDataOutputStream createNonRecursiveRemote(Path path, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException;

  /**
   * Create remote file (non-recursive)
   * @param f path
   * @param permission file permission
   * @param overwrite overwrite file
   * @param bufferSize buffer size
   * @param replication replication factor
   * @param blockSize block size
   * @param progress progress listener
   * @return output stream
   * @throws IOException
   */
  public FSDataOutputStream createNonRecursiveRemote(Path path, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
      throws IOException;

  /**
   * Append remote file
   * @param f path
   * @param bufferSize buffer size
   * @param progress progress listener
   * @return output stream
   * @throws IOException
   */
  public FSDataOutputStream appendRemote(Path f, int bufferSize, Progressable progress)
      throws IOException;

  /**
   * Rename remote file
   * @param src source
   * @param dst destination
   * @return true on success, false - otherwise
   * @throws IOException
   */
  public boolean renameRemote(Path src, Path dst) throws IOException;

  /**
   * Rename remote file with options
   * @param src source
   * @param dst destination
   * @param options rename options
   * @throws IOException
   */
  public void renameRemote(Path src, Path dst, Rename... options) throws IOException;
  
  /**
   * Delete remote file or directory
   * @param f file path
   * @param recursive is operation recursive
   * @return true on success, false  - otherwise
   * @throws IOException
   */
  public boolean deleteRemote(Path f, boolean recursive) throws IOException;

  /**
   * Make all directories remote
   * @param path path to a directory
   * @param permission file permission
   * @return true on success, false  - otherwise
   * @throws IOException
   * @throws FileAlreadyExistsException
   */
  public boolean mkdirsRemote(Path path, FsPermission permission)
      throws IOException, FileAlreadyExistsException;

  /**
   * Create file remote
   * @param f file path
   * @param permission file permission
   * @param cflags create flags
   * @param bufferSize buffer size
   * @param replication replication
   * @param blockSize block size
   * @param progress progress
   * @param checksumOpt checksum 
   * @return data output stream
   * @throws IOException 
   */
  public FSDataOutputStream createRemote(Path f, FsPermission permission,
      EnumSet<CreateFlag> cflags, int bufferSize, short replication, long blockSize,
      Progressable progress, ChecksumOpt checksumOpt) throws IOException;

}
