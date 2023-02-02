/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.carrot.sidecar.abfs;

import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;

/**
 * Sidecar caching file system for secure ABFS
 * fs.abfss.impl=com.carrot.sidecar.abfs.SecureSidecarAzureBlobFileSystem
 */
public class SecureSidecarAzureBlobFileSystem extends SidecarAzureBlobFileSystem {

  @Override
  public boolean isSecureScheme() {
    return true;
  }

  @Override
  public String getScheme() {
    return FileSystemUriSchemes.ABFS_SECURE_SCHEME;
  }
}
