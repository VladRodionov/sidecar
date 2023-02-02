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
package com.carrot.sidecar;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public interface CachingFileSystem {
  
  public SidecarCachingFileSystem getCachingFileSystem();
  
  public FSDataInputStream openRemote(Path f, int bufferSize) throws IOException;
  
  public FSDataOutputStream createRemote(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException; 

  public FSDataOutputStream createNonRecursiveRemote(Path path, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException; 

  public FSDataOutputStream createNonRecursiveRemote(Path path, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException;
  
  public FSDataOutputStream appendRemote(Path f, int bufferSize, Progressable progress)
      throws IOException; 

  public boolean renameRemote(Path src, Path dst) throws IOException;
 
  public boolean deleteRemote(Path f, boolean recursive) throws IOException;
  
  public boolean mkdirsRemote(Path path, FsPermission permission)
      throws IOException, FileAlreadyExistsException; 
  
}
