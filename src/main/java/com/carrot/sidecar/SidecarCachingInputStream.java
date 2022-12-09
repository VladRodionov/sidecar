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
package com.carrot.sidecar;

import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import com.carrot.cache.Cache;
import com.carrot.cache.io.ObjectPool;
import com.carrot.cache.util.Utils;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class SidecarCachingInputStream extends InputStream 
  implements Seekable, PositionedReadable, ByteBufferReadable{

  private static final Logger LOG = LoggerFactory.getLogger(SidecarCachingInputStream.class);

  /** The length of the external file*/
  private long fileLength;
  
  /** Key base for all data pages in this external file*/
  private byte[] baseKey;
  
  /** The external file input stream future*/
  private Callable<FSDataInputStream> remoteStreamCallable;
  
  /** Remote input stream */
  private FSDataInputStream remoteStream;
  
  /** Cached input stream future - can be null*/
  private Callable<FSDataInputStream> cacheStreamCallable;
  
  /** Cache input stream (from write cache FS, if enabled)*/
  private FSDataInputStream cacheStream;
  
  /** Carrot cache instance */
  private Cache cache;
  
  /** Page size in bytes. */
  private int pageSize;
  
  /** I/O buffer size in bytes */
  private int bufferSize;

  /** I/O buffer to read from cache*/
  private byte[] buffer = null;
  
  /* Used to read page from the external stream */
  private byte[] extBuffer = null;
  
  /** I/O buffer start offset in the file*/
  private long bufferStartOffset;
  
  /** I/O buffer end offset in the file*/
  private long bufferEndOffset;
 
  /** Current position of the stream, relative to the start of the file. */
  private long position = 0;
  
  /** Closed flag */
  private boolean closed = false;
  
  /** End of file reached */
  private boolean EOF = false;
  
  /* Pool which keeps I/O buffers to read from cache directly */
  private static ObjectPool<byte[]> ioPool = new ObjectPool<byte[]>(32);
  
  /* Pool which keeps page buffers to read from external source */
  private static ObjectPool<byte[]> pagePool = new ObjectPool<byte[]>(32);
  
  static synchronized void initIOPools(int size) {
    if (ioPool == null || ioPool.getMaxSize() != size) {
      ioPool = new ObjectPool<byte[]>(size);
    }
    if (pagePool == null || pagePool.getMaxSize() != size) {
      pagePool = new ObjectPool<byte[]>(size);
    }
  }
  
  /**
   * Constructor 
   * @param cache parent cache
   * @param path file path
   * @param remoteStreamCall external input stream callable
   * @param ccheStreamFuture cache input stream callable
   * @param fileLength file length
   * @param pageSize page size
   * @param bufferSize I/O buffer size
   */
  public SidecarCachingInputStream(Cache cache, Path path, Callable<FSDataInputStream> remoteStreamCall, 
      Callable<FSDataInputStream> cacheStreamCall,
      long fileLength, int pageSize, int bufferSize) {
    this.cache = cache;
    this.remoteStreamCallable = remoteStreamCall;
    this.cacheStreamCallable = cacheStreamCall;
    this.pageSize = pageSize;
    this.bufferSize = bufferSize;
    this.fileLength = fileLength;
    initBaseKey(path);
    // Must always be > 0 for performance 
    this.buffer = getIOBuffer();
    this.extBuffer = getPageBuffer();
  }
  
  private byte[] getPageBuffer() {
    byte[] buffer = pagePool.poll();
    if (buffer == null) {
      buffer = new byte[pageSize];
    }
    return buffer;
  }
  
  private byte[] getIOBuffer() {
    if (bufferSize > 0) {
      byte[] buffer = ioPool.poll();
      if (buffer == null) {
        buffer = new byte[bufferSize];
      }
      return buffer;
    }
    return null;
  }
  
  private void initBaseKey(Path path) {
    String hash =  md5().hashString(path.toString(), UTF_8).toString();
    this.baseKey = new byte[hash.length() + Utils.SIZEOF_LONG];
    System.arraycopy(hash.getBytes(), 0, this.baseKey, 0, hash.length());
  }
  
  @Override
  public int read(byte[] bytesBuffer, int offset, int length) throws IOException {
    return readInternal(bytesBuffer, offset, length, position,
        false);
  }

  public int read(ByteBuffer buffer, int offset, int length) throws IOException {
    
    if (buffer.hasArray()) {
      byte[] buf  = buffer.array();
      int totalBytesRead = readInternal(buf, offset, length, position, false);
      if (totalBytesRead < 0) {
        return -1;
      }
      buffer.position(offset + totalBytesRead);
      return totalBytesRead;
    }
    
    byte[] bytesBuffer = length <= bufferSize? getIOBuffer(): new byte[length];
    int totalBytesRead =
        readInternal(bytesBuffer, 0, length, position, false);
    if (totalBytesRead == -1) {
      return -1;
    }
    buffer.position(offset);
    buffer.put(bytesBuffer, 0, totalBytesRead);
    if (bytesBuffer.length == bufferSize) {
      // release buffer back to the I/O pool
      ioPool.offer(bytesBuffer);
    }
    return totalBytesRead;
  }

  private int bufferedRead(byte[] bytesBuffer, int offset, int length,
                           long position) throws IOException {
    if (buffer == null) { //buffer is disabled, read data from local cache directly.
      return localCachedRead(bytesBuffer, offset, length, position);
    }
    //hit or partially hit the in stream buffer
    if (position >= bufferStartOffset && position < bufferEndOffset) {
      int lengthToReadFromBuffer = (int) Math.min(length,
          bufferEndOffset - position);
      System.arraycopy(buffer, (int) (position - bufferStartOffset),
          bytesBuffer, offset, lengthToReadFromBuffer);
      
      return lengthToReadFromBuffer;
    }
    if (length >= bufferSize) {
      // Skip load to the in stream buffer if the data piece is larger than buffer size
      return localCachedRead(bytesBuffer, offset, length, position);
    }
    int bytesLoadToBuffer = (int) Math.min(bufferSize, this.fileLength - position);
    int bytesRead =
        localCachedRead(buffer, 0, bytesLoadToBuffer, position);
    bufferStartOffset = position;
    bufferEndOffset = position + bytesRead;
    int dataReadFromBuffer = Math.min(bytesRead, length);
    System.arraycopy(buffer, 0, bytesBuffer, offset, dataReadFromBuffer);
    
    return dataReadFromBuffer;
  }

  private int localCachedRead(byte[] bytesBuffer, int offset, int length, 
                              long position) throws IOException {
    long currentPage = position / pageSize;
   
    int currentPageOffset = (int) (position % pageSize);
    // This is the assumption which is not always correct ???
    int bytesLeftInPage = (int) (pageSize - currentPageOffset);
    int bytesToReadInPage = Math.min(bytesLeftInPage, length);
    byte[] key = getKey(currentPage * pageSize);
    
    int bytesRead = (int) cache.getRange(key, 0, key.length, currentPageOffset, bytesToReadInPage,
        true, bytesBuffer, offset);
    if (bytesRead > 0) {
      return bytesRead;
    }
    // on local cache miss, read a  from external storage. This will always make
    // progress or throw an exception
    // This is assumption that external buffer size == page size
    int size = readExternalPage(position);
    if (size > 0) {
      cache.put(key, 0, key.length, this.extBuffer, 0, size, 0L /* no expire */);
      bytesToReadInPage = Math.min(bytesToReadInPage, size - currentPageOffset);
      System.arraycopy(this.extBuffer, currentPageOffset, bytesBuffer, offset, bytesToReadInPage);
      // Can be negative
      return bytesToReadInPage < 0? 0: bytesToReadInPage;
    }
    return 0;
  }

  private int readInternal(byte[] bytesBuffer, int offset, int length,
                           long position, boolean isPositionedRead) throws IOException {
    Preconditions.checkArgument(length >= 0, "length should be non-negative");
    Preconditions.checkArgument(offset >= 0, "offset should be non-negative");
    Preconditions.checkArgument(position >= 0, "position should be non-negative");
    
    if (length == 0) {
      return 0;
    }
    if (position >= this.fileLength) { // at the end of file
      return -1;
    }
    int totalBytesRead = 0;
    long currentPosition = position;
    long lengthToRead = Math.min(length, this.fileLength - position);
    int bytesRead = 0;
    while (totalBytesRead < lengthToRead) {
      bytesRead = bufferedRead(bytesBuffer, offset + totalBytesRead,
          (int) (lengthToRead - totalBytesRead), currentPosition);
      totalBytesRead += bytesRead;
      currentPosition += bytesRead;
      if (!isPositionedRead) {
        this.position = currentPosition;
      }
    }
    // Update position of the external stream
    if (!isPositionedRead) {
      getRemoteStream().seek(this.position);
      FSDataInputStream cacheStream = getCacheStream();
      if (cacheStream != null) {
        try {
          cacheStream.seek(this.position);
        } catch (IOException e) {
          //TODO: better handling exception
          LOG.error("Cached input stream", e);
          this.cacheStreamCallable = null;
          this.cacheStream = null;
        }
      }
    }
    if (totalBytesRead > length
        || (totalBytesRead < length && currentPosition < this.fileLength)) {
      throw new IOException(String.format("Invalid number of bytes read - "
          + "bytes to read = %d, actual bytes read = %d, bytes remains in file %d last read=%d",
          length, totalBytesRead, remaining(), bytesRead));
    }
    return totalBytesRead;
  }

  private FSDataInputStream getCacheStream() throws IOException {
    try {
      if (this.cacheStreamCallable == null) {
        return null;
      }
      if (this.cacheStream != null) {
        return this.cacheStream;
      }
      this.cacheStream =  this.cacheStreamCallable.call();
      return this.cacheStream;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private FSDataInputStream getRemoteStream() throws IOException {
    try {
      if (this.remoteStreamCallable == null) {
        return null;
      }
      if (this.remoteStream != null) {
        return this.remoteStream;
      }
      this.remoteStream =  this.remoteStreamCallable.call();
      return this.remoteStream;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
  
  @Override
  public long skip(long n) {
    checkIfClosed();
    if (n <= 0) {
      return 0;
    }
    long toSkip = Math.min(remaining(), n);
    position += toSkip;
    return toSkip;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      throw new IOException("Cannot close a closed stream");
    }
    getRemoteStream().close();
    FSDataInputStream cached = getCacheStream();
    if (cached != null) {
      try {
        cached.close();
      } catch (IOException e) {
        LOG.error("Cached input stream", e);
      }
    }
    closed = true;
    // release buffers
    if (buffer != null) {
      ioPool.offer(buffer);
    }
    if (extBuffer != null) {
      pagePool.offer(extBuffer);
    }
  }

  public long remaining() {
    return EOF ? 0 : this.fileLength - position;
  }

  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    return readInternal(b, off, len,  pos, true);
  }

  @Override
  public long getPos() {
    return position;
  }

  @Override
  public void seek(long pos) {
    checkIfClosed();
    Preconditions.checkArgument(pos >= 0, "Seek position is negative: %s", pos);
    Preconditions
        .checkArgument(pos <= this.fileLength,
            "Seek position (%s) exceeds the length of the file (%s)", pos, this.fileLength);
    if (pos == position) {
      return;
    }
    if (pos < position) {
      EOF = false;
    }
    position = pos;
  }

  /**
   * Convenience method to ensure the stream is not closed.
   */
  private void checkIfClosed() {
    Preconditions.checkState(!closed, "Cannot operate on a closed stream");
  }

  private synchronized int readExternalPage(long position)
      throws IOException {
    long pageStart = position - (position % this.pageSize);
    int pageSize = (int) Math.min(this.pageSize, this.fileLength - pageStart);
    byte[] page = this.extBuffer;
    int totalBytesRead = 0;
    while (totalBytesRead < pageSize) {
      int bytesRead;
      bytesRead = readExternalPage(pageStart + totalBytesRead, page, totalBytesRead, pageSize - totalBytesRead);
      if (bytesRead <= 0) {
        break;
      }
      totalBytesRead += bytesRead;
    }
 
    if (totalBytesRead != pageSize) {
      throw new IOException("Failed to read page from external storage. Bytes read: "
          + totalBytesRead + " Page size: " + pageSize);
    }
    return totalBytesRead;
  }

  private int readExternalPage(long offset, byte[] buffer, int bufOffset, int len) throws IOException {
    int read = readFromCache(offset, buffer, bufOffset, len);
    if (read > 0) {
      return read;
    }
    return readFromRemote(offset, buffer, bufOffset, len);
  }
  
  private int readFromRemote(long offset, byte[] buffer, int bufOffset, int len)
      throws IOException {
    FSDataInputStream is = getRemoteStream();
    return is.read(offset, buffer, bufOffset, len);
  }
  
  private int readFromCache(long offset, byte[] buffer, int bufOffset, int len) {
    if (this.cacheStreamCallable == null) {
      return -1;//
    }
    try {
      FSDataInputStream is = getCacheStream();
      return is.read(offset, buffer, bufOffset, len);
    } catch(IOException e) {
      //TODO: better exception handling
      LOG.error("Cached input stream", e);
      this.cacheStreamCallable = null;
      this.cacheStream = null;
    }
    return -1;
  }
  
  
  public int available() throws IOException {
    if (this.closed) {
      throw new IOException("Cannot query available bytes from a closed stream.");
    }
    return (int) remaining();
  }

  private byte[] one = new byte[1];

  @Override
  public int read() throws IOException {
    if (this.closed) {
      throw new IOException("Cannot read from a closed stream");
    }

    int n = read(one, 0, 1);
    if (n == -1) {
      return n;
    }
    return one[0] & 0xff;
  }

  @Override
  public int read(byte[] buffer) throws IOException {
    return read(buffer, 0, buffer.length);
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    if (this.closed) {
      throw new IOException("Cannot read from a closed stream");
    }
    int bytesRead = read(buf, buf.position(), buf.remaining());
    
    return bytesRead;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    if (this.closed) {
      throw new IOException("Cannot read from a closed stream");
    }

    int bytesRead = positionedRead(position, buffer, offset, length);
    
    return bytesRead;
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    int totalBytesRead = 0;
    while (totalBytesRead < length) {
      int bytesRead =
          read(position + totalBytesRead, buffer, offset + totalBytesRead, length - totalBytesRead);
      if (bytesRead == -1) {
        LOG.error("file length={} position={} totalBytesRead={} to read={}", 
          fileLength, position, totalBytesRead, length - totalBytesRead);
        throw new EOFException();
      }
      totalBytesRead += bytesRead;
    }
  }

  /**
   * This method is not supported in {@link SidecarCachingInputStream}.
   *
   * @param targetPos N/A
   * @return N/A
   * @throws IOException always
   */
  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException("This method is not supported.");
  }
  
  private byte[] getKey(long offset) {
    int size = this.baseKey.length;
    offset = offset / pageSize * pageSize;
    for (int i = 0; i < Utils.SIZEOF_LONG; i++) {
      int rem = (int) (offset % 256);
      this.baseKey[size - i - 1] = (byte) rem;
      offset /= 256;
    }
    return this.baseKey;
  }
  
}
