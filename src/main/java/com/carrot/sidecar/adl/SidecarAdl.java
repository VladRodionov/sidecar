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
package com.carrot.sidecar.adl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.adl.AdlFileSystem;

/**
 * Sidecar - backed Azure  Data Lake File System (Gen 1) implementation of AbstractFileSystem.
 * This impl delegates to the SidecarAdlFileSystem. This is used 
 * from inside YARN containers to access Hadoop - compatible file system
 * 
 * Hadoop configuration:
 * fs.AbstractFileSystem.adl.impl=com.carrot.sidecar.adl.SidecarAdl
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SidecarAdl extends DelegateToFileSystem{

  public SidecarAdl(URI theUri, Configuration conf) throws IOException, URISyntaxException {
    super(theUri, new SidecarAdlFileSystem(), conf, AdlFileSystem.SCHEME, false);
  }

  @Override
  public int getUriDefaultPort() {
    return 443; // AdlFileSystem.DEFAULT_PORT - not visible
  }
}
