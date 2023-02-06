package com.carrot.sidecar.adl;

import java.io.IOException;
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
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.adl.AdlFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.carrot.sidecar.RemoteFileSystemAccess;
import com.carrot.sidecar.MetaDataCacheable;
import com.carrot.sidecar.SidecarCachingFileSystem;

/**
 * 
 * Sidecar caching FS for Azure Data Lake File System Gen 1
 * fs.adl.impl=com.carrot.sidecar.adl.SidecarAdlFileSystem
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SidecarAdlFileSystem extends AdlFileSystem 
  implements MetaDataCacheable, RemoteFileSystemAccess{
  
  private SidecarCachingFileSystem sidecar;
  
  public SidecarAdlFileSystem() {}
  
  @Override
  public void initialize(URI name, Configuration originalConf) throws IOException {
    super.initialize(name, originalConf);
    this.sidecar = SidecarCachingFileSystem.get(this);
    //TODO: do we need to initialize if it was cached? 
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
  
  /**
   * ADL Gen 1 supports this API
   */
  @Override
  public void concat(Path trg, Path[] srcs) throws IOException {
    sidecar.concat(trg, srcs);
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
  public void rename(Path src, Path dst, Rename ... options) throws IOException {
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
   * 
   *  CachingFileSystem interface
   * 
   */

  @Override
  public SidecarCachingFileSystem getCachingFileSystem() {
    return sidecar;
  }
  
  @Override
  public void concatRemote(Path trg, Path[] pathes) throws IOException {
    super.concat(trg, pathes);
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

  @SuppressWarnings("deprecation")
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
}
