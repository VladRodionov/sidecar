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
package com.carrot.sidecar.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

/**
 * Sidecar - backed HDFS implementation of AbstractFileSystem.
 * This impl delegates to the SidecarDistributedFileSystem. This is used 
 * from inside YARN containers to access Hadoop - compatible file systems
 * 
 * Hadoop configuration:
 * fs.AbstractFileSystem.hdfs.impl=com.carrot.sidecar.hdfs.SidecarHdfs
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SidecarHdfs extends DelegateToFileSystem{

  public SidecarHdfs(URI theUri, Configuration conf) throws IOException, URISyntaxException {
    super(theUri, new SidecarDistributedFileSystem(), conf, HdfsConstants.HDFS_URI_SCHEME, false);
  }

  @Override
  public int getUriDefaultPort() {
    return HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT;
  }
}
