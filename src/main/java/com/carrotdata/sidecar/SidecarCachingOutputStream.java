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
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;

@NotThreadSafe
public class SidecarCachingOutputStream extends OutputStream 
implements Syncable, CanSetDropBehind, StreamCapabilities{

  /* 
   * SidecarCachingOutputStream listener interface
   */
  public static interface Listener {
    
    /* Report number of bytes written */
    public void bytesWritten(SidecarCachingOutputStream stream, long bytes);
    
    /* Notify that the stream is about to be closed*/
    public void closingRemote(SidecarCachingOutputStream stream);
    
    /* Report exception */
    public void reportException(SidecarCachingOutputStream stream, Exception e);
  }
  
  /*
   * Output stream listener - SidecarCachingFileSystem
   */
  private Listener listener; 
  
  /*
   * Remote path to the file
   */
  private Path remotePath;
  
  /*
   * Current length
   */
  private transient long length;
  
  /*
   * Caching output stream
   */
  private FSDataOutputStream cachingOut; // can be null?
  
  /*
   * Remote output stream 
   */
  private FSDataOutputStream remoteOut; // remote stream
  
  /**
   * Constructor
   * @param c caching output stream - can be null
   * @param r remote output stream
   * @param p path to the remote file
   */
  public SidecarCachingOutputStream(FSDataOutputStream c, FSDataOutputStream r, Path p, Listener l) {
    this.cachingOut = c;
    this.remoteOut = r;
    this.remotePath = p;
    this.listener = l;
  }
  
  /**
   * Get current length of the file
   * @return length
   * @throws IOException 
   */
  public long length() throws IOException {
    return this.remoteOut.getPos();
  }
  
  /**
   * Get caching output stream
   * @return caching output stream
   */
  public FSDataOutputStream getCachingStream() {
    return this.cachingOut;
  }
  
  /**
   * Get remote output stream
   * @return remote output stream
   */
  public FSDataOutputStream getRemoteStream() {
    return this.remoteOut;
  }
  
  /**
   * Get remote file path
   * @return remote file path
   */
  public Path getRemotePath() {
    return this.remotePath;
  }
  
  /**
   * Disable caching stream in case of write failure 
   */
  public void disableCachingStream() {
    this.cachingOut = null;
  }
  
  /**
   * Set stream's listener
   * @param l listener (SidecarCachingFileSystem)
   */
  public void setListener(Listener l) {
    this.listener = l;
  }
  
  @Override
  public void close() throws IOException {
    closeCachingStream();
    listener.closingRemote(this);
  }

  /**
   * Close caching stream
   * @throws IOException
   */
  void closeCachingStream() throws IOException {
    if (this.cachingOut != null) {
      try {
        this.cachingOut.close();
      } catch (IOException e) {
        listener.reportException(this, e);
      }
    }
  }
  
  /**
   * Close remote stream
   * @throws IOException
   */
  void closeRemoteStream() throws IOException {
    this.remoteOut.close();
  }
  
  @Override
  public void flush() throws IOException {
    flushCaching();
    this.remoteOut.flush();
  }

  private void flushCaching() throws IOException {
    if (this.cachingOut != null) {
      try {
        this.cachingOut.flush();
      } catch (IOException e) {
        listener.reportException(this, e);
      }
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    writeCaching(b, off, len);
    this.remoteOut.write(b, off, len);
    incrLength(len);
  }

  private void writeCaching(byte[] b, int off, int len) {
    if (this.cachingOut != null) {
      try {
        cachingOut.write(b, off, len);
      } catch (IOException e) {
        if (this.listener != null) {
          this.listener.reportException(this, e);
          // Listener will close caching stream and delete the file
        }
      } 
    }
  }
  
  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(int arg) throws IOException {
    writeCaching(arg);
    this.remoteOut.write(arg);
    incrLength(1);
  }

  private void writeCaching(int arg) {
    if (this.cachingOut != null) {
      try {
        cachingOut.write(arg);
      } catch (IOException e) {
        if (this.listener != null) {
          this.listener.reportException(this, e);
          // Listener will close caching stream and delete the file
        }
      } 
    }
  }
  
  @Override
  public boolean hasCapability(String capability) {
    return cachingOut == null? remoteOut.hasCapability(capability): 
      cachingOut.hasCapability(capability) && remoteOut.hasCapability(capability);
  }

  @Override
  public void setDropBehind(Boolean dropCache) throws IOException, UnsupportedOperationException {
    setDropBehindCaching(dropCache);
    remoteOut.setDropBehind(dropCache);
  }

  private void setDropBehindCaching(Boolean dropCache) throws IOException, UnsupportedOperationException {
    if (cachingOut != null) {
      try {
        cachingOut.setDropBehind(dropCache);
      } catch (Exception e) {
        // swallow
      }
    }
  }
  
  @SuppressWarnings("deprecation")
  @Override
  public void sync() throws IOException {
    syncCaching();
    remoteOut.sync();
  }
  
  @SuppressWarnings("deprecation")
  private void syncCaching() {
    if (cachingOut != null) {
      try {
      cachingOut.sync();
      } catch (IOException e) {
        listener.reportException(this, e);
      }
    }
  }
  
  @Override
  public void hflush() throws IOException {
    hflushCaching();
    remoteOut.hflush();    
  }
  
  private void hflushCaching() {
    if (cachingOut != null) {
      try {
      cachingOut.hflush();
      } catch (IOException e) {
        listener.reportException(this, e);
      }
    }
  }
  
  @Override
  public void hsync() throws IOException {
    hsyncCaching();
    remoteOut.hsync();   
  }
  
  private void hsyncCaching() {
    if (cachingOut != null) {
      try {
      cachingOut.hsync();
      } catch (IOException e) {
        listener.reportException(this, e);
      }
    }
  }
  
  private void incrLength(long incr) {
    this.length += incr;
    this.listener.bytesWritten(this, incr);
  }
}
