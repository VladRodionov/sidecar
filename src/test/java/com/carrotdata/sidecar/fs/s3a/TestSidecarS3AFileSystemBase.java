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
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.carrotdata.sidecar.RemoteFileSystemAccess;
import com.carrotdata.sidecar.SidecarCachingFileSystem;
import com.carrotdata.sidecar.SidecarDataCacheType;
import com.carrotdata.sidecar.TestCachingFileSystemBase;
import com.carrotdata.sidecar.fs.s3a.SidecarS3AFileSystem;

public abstract class TestSidecarS3AFileSystemBase  extends TestCachingFileSystemBase{

  /**
   * Minio server access
   */
  protected String S3_ENDPOINT = "http://localhost:9000";
  protected String S3_BUCKET = "s3a://test";
  protected String ACCESS_KEY = "admin";
  protected String SECRET_KEY = "password";
  
  protected SidecarDataCacheType cacheType = SidecarDataCacheType.FILE;
  boolean useWriteCache = true;
  

  @Override
  protected FileSystem cachingFileSystem() throws IOException {
    Configuration configuration = getConfiguration();
    try {
      return cachingFileSystemS3A(configuration);
    } catch (URISyntaxException e) {
      LOG.error(e.getMessage());
      return null;
    }
  }
  
  private FileSystem cachingFileSystemS3A(Configuration configuration)
      throws URISyntaxException, IOException {
    
 
    String disableCacheName = "fs.s3a.impl.disable.cache";
    configuration.set("fs.s3a.impl", SidecarS3AFileSystem.class.getName());
    configuration.setBoolean(disableCacheName, true);
    
    configuration.set("fs.s3a.access.key", ACCESS_KEY);
    configuration.set("fs.s3a.secret.key", SECRET_KEY);
    configuration.setBoolean("fs.s3a.path.style.access", true);
    configuration.set("fs.s3a.block.size", "512M");
    configuration.setBoolean("fs.s3a.committer.magic.enabled", false);
    configuration.set("fs.s3a.committer.name", "directory");
    configuration.setBoolean("fs.s3a.committer.staging.abort.pending.uploads", true);
    configuration.set("fs.s3a.committer.staging.conflict-mode","append");
    configuration.setBoolean("fs.s3a.committer.staging.unique-filenames", true);
    configuration.setInt("fs.s3a.connection.establish.timeout", 5000);
    configuration.setBoolean("fs.s3a.connection.ssl.enabled", false);
    configuration.setInt("fs.s3a.connection.timeout", 200000);
    configuration.set("fs.s3a.endpoint", S3_ENDPOINT);

    configuration.setInt("fs.s3a.committer.threads", 64);// Number of threads writing to MinIO
    configuration.setInt("fs.s3a.connection.maximum", 8192);// Maximum number of concurrent conns
    configuration.setInt("fs.s3a.fast.upload.active.blocks", 2048);// Number of parallel uploads
    configuration.set("fs.s3a.fast.upload.buffer", "disk");//Use drive as the buffer for uploads
    configuration.setBoolean("fs.s3a.fast.upload", true);//Turn on fast upload mode
    configuration.setInt("fs.s3a.max.total.tasks", 2048);// Maximum number of parallel tasks
    configuration.set("fs.s3a.multipart.size", "64M");//  Size of each multipart chunk
    configuration.set("fs.s3a.multipart.threshold", "64M");//Size before using multipart uploads
    configuration.setInt("fs.s3a.socket.recv.buffer", 65536);// Read socket buffer hint
    configuration.setInt("fs.s3a.socket.send.buffer", 65536);// Write socket buffer hint
    configuration.setInt("fs.s3a.threads.max", 256);//  Maximum number of threads for S3A
    
    FileSystem testingFileSystem = FileSystem.get(new URI(S3_BUCKET), configuration);
    //testingFileSystem.setWorkingDirectory(new Path("/test"));
    
    SidecarCachingFileSystem cachingFileSystem = ((RemoteFileSystemAccess) testingFileSystem).getCachingFileSystem();
    cachingFileSystem.setMetaCacheEnabled(true);
    
    return testingFileSystem;
  }
}
