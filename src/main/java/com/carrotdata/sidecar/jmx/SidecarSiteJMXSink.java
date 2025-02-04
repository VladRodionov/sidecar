package com.carrotdata.sidecar.jmx;

import java.net.URI;

import com.carrotdata.sidecar.SidecarCachingFileSystem;

public class SidecarSiteJMXSink implements SidecarSiteJMXSinkMBean{

  public SidecarSiteJMXSink() {
  }

  @Override
  public long getwritecache_max_size() {
    return SidecarCachingFileSystem.getWriteCacheMaxSize();
  }

  @Override
  public long getwrite_cache_current_size() {
    return SidecarCachingFileSystem.getCurrentWriteCacheSize();
  }

  @Override
  public double getwrite_cache_used_ratio() {
    long maxSize = SidecarCachingFileSystem.getWriteCacheMaxSize();
    long current = SidecarCachingFileSystem.getCurrentWriteCacheSize();
    return (double) current/ maxSize;
  }

  @Override
  public long getwrite_cache_number_files() {
    return SidecarCachingFileSystem.getNumberFilesInWriteCache();
  }

  @Override
  public int getpending_io_tasks() {
    return SidecarCachingFileSystem.getTaskQueueSize();
  }

  @Override
  public String getwrite_cache_uri() {
    URI uri = SidecarCachingFileSystem.getWriteCacheURI();
    if (uri != null) {
      return uri.toString();
    }
    return "";
  }

  @Override
  public long getwrite_cache_bytes_written() {
    return SidecarCachingFileSystem.getWriteCacheBytesWritten();
  }

  @Override
  public long getdata_set_size_disk() {
    return SidecarCachingFileSystem.getDataSetSizeOnDisk();
  }

}
