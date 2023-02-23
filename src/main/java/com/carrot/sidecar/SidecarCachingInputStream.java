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

import static com.carrot.sidecar.util.Utils.checkArgument;
import static com.carrot.sidecar.util.Utils.checkState;
import static com.carrot.sidecar.util.Utils.getBaseKey;
import static com.carrot.sidecar.util.Utils.getKey;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrot.cache.Cache;
import com.carrot.cache.io.ObjectPool;
import com.carrot.sidecar.hints.CacheOnReadHint;
import com.carrot.sidecar.util.ScanDetector;
import com.carrot.sidecar.util.Statistics;

@NotThreadSafe
public class SidecarCachingInputStream extends InputStream 
  implements Seekable, PositionedReadable, ByteBufferReadable{
  
  /**
   * Convenient class to keep file segment range
   */
  static class Range {
    private long start;
    private long size;
    Range (long start, long size){
      this.start = start;
      this.size = size;
    }
    long getStart() {
      return this.start;
    }
    long size() {
      return this.size;
    }
  }
  
  private static final Logger LOG = LoggerFactory.getLogger(SidecarCachingInputStream.class);

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
   * Path to the remote file
   */
  private Path path;
  
  /** The length of the remote file */
  private long fileLength;
  
  /** Is this file cacheable */
  private boolean cacheOnRead;
  
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
  private byte[] pageBuffer = null;
  
  /** I/O buffer start offset in the file*/
  private long bufferStartOffset;
  
  /** I/O buffer end offset in the file*/
  private long bufferEndOffset;
 
  /** 
   * Current position of the stream, relative to the start of the file. 
   * This position is the source of truth for write cache stream and for remote stream
   * When read is non - positional 
   * */
  private volatile long position = 0;
  
  /** Closed flag */
  private volatile boolean closed = false;
  
  /** End of file reached */
  private volatile boolean EOF = false;
  
  /** Just 'one' byte */
  private byte[] one = new byte[1];

  /** Statistics collector*/
  private Statistics stats;
  
  /** TODO: real scan detector */
  private ScanDetector sd;
  
  /** Cache on read hint class */
  private CacheOnReadHint hint;
  
  /**
   * Constructor 
   * @param cache parent cache
   * @param status file status
   * @param remoteStreamCall external input stream callable
   * @param cacheStreamCall cache input stream callable
   * @param pageSize page size
   * @param bufferSize I/O buffer size (at least as large as page size)
   * @param stats statistics agent
   * @param sd scan detector to detect long scan operations
   * @param cacheOnRead can cache?
   */
  public SidecarCachingInputStream(Cache cache, FileStatus status, Callable<FSDataInputStream> remoteStreamCall, 
      Callable<FSDataInputStream> cacheStreamCall,
       int pageSize, int bufferSize, Statistics stats, boolean cacheOnRead, ScanDetector sd) {
    this(cache, status.getPath(), remoteStreamCall, cacheStreamCall, status.getModificationTime(), 
      status.getLen(), pageSize, bufferSize, stats, cacheOnRead, sd);
  }
  
  /**
   * Constructor 
   * @param cache parent cache
   * @param path file path
   * @param remoteStreamCall external input stream callable
   * @param cacheStreamCall cache input stream callable
   * @param modTime modification time
   * @param fileLength file length
   * @param pageSize page size
   * @param bufferSize I/O buffer size (at least as large as page size)
   * @param stats statistics agent
   * @param sd scan detector to detect long scan operations
   * @param cacheOnRead can cache?
   */
  public SidecarCachingInputStream(Cache cache, Path path, Callable<FSDataInputStream> remoteStreamCall, 
      Callable<FSDataInputStream> cacheStreamCall, long modTime, long fileLength,
       int pageSize, int bufferSize, Statistics stats, boolean cacheOnRead, ScanDetector sd) {
    this.cache = cache;
    this.remoteStreamCallable = remoteStreamCall;
    this.cacheStreamCallable = cacheStreamCall;
    this.pageSize = pageSize;
    this.bufferSize = bufferSize;
    // Adjust I/O buffer size
    this.bufferSize = this.bufferSize / this.pageSize * this.pageSize;
    if (this.bufferSize < bufferSize || bufferSize == 0) {
      this.bufferSize += this.pageSize;
    }
    this.path = path;
    this.fileLength = fileLength;
    this.baseKey = getBaseKey(path, modTime);
    // Must always be > 0 for performance 
    this.buffer = getIOBuffer();
    this.pageBuffer = getPageBuffer();
    this.stats = stats;
    this.cacheOnRead = cacheOnRead;
    this.sd = sd;
    this.hint = CacheOnReadHint.fromConfig(SidecarConfig.getInstance());
  }
  
  private boolean cacheOnReadThread() {
    if (this.hint != null) {
      return hint.cacheOnReadThread();
    }
    return true;
  }
  
  /**
   * Get statistics on this stream
   * @return
   */
  public Statistics getStatistics() {
    return this.stats;
  }
  /**
   * Get file path
   * @return file path
   */
  public Path getFilePath() {
    return this.path;
  }
    
  /**
   * Get I/O buffer from the pool
   * @return buffer
   */
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
  
  /** 
   * Get page buffer from the pool
   * @return buffer
   */
  private byte[] getPageBuffer() {
    byte[] buffer = pagePool.poll();
    if (buffer == null) {
      buffer = new byte[pageSize];
    }
    return buffer;
  }

  /**
   * Public API section
   */
  @Override
  public synchronized int read(byte[] bytesBuffer, int offset, int length) throws IOException {
    checkIfClosed();
    this.stats.addTotalReadRequests(1);
    int read = readInternal(bytesBuffer, offset, length, position,
        false);
    if (read > 0) {
      this.stats.addTotalBytesRead(read);
    }
    return read;
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    checkIfClosed();
    if (n <= 0) {
      return 0;
    }
    long toSkip = Math.min(remaining(), n);
    position += toSkip;
    //TODO: do we need to keep cache and remote stream in sync?
    getRemoteStream().skip(toSkip);
    this.cacheStream = getCacheStream();
    if (this.cacheStream != null) {
      try {
        this.cacheStream.skip(toSkip);
      } catch(IOException e) {
        //TODO: better handling exception
        LOG.error("Cached input stream skip", e);
        this.cacheStream = null;
        this.cacheStreamCallable = null;
      }
    }
    return toSkip;
  }

  @Override
  public void close() throws IOException {

    // Do not throw exception
    if (closed) {
      return;
    }
    try {
      getRemoteStream().close();
    } catch (IOException e) {
      LOG.error("Remote file {}", path);
      LOG.error("Remote input stream close failed", e);
    }
    FSDataInputStream cached = getCacheStream();
    if (cached != null) {
      try {
        cached.close();
      } catch (IOException e) {
        LOG.error("Remote file {}", path);
        LOG.error("Write cache input stream close failed", e);
      }
    }
    closed = true;
    // release buffers
    if (buffer != null) {
      ioPool.offer(buffer);
    }
    if (pageBuffer != null) {
      pagePool.offer(pageBuffer);
    }
  }

  @Override
  public synchronized long getPos() {
    return position;
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    checkIfClosed();
    checkArgument(pos >= 0, "Seek position is negative: " + pos);
    checkArgument(pos <= this.fileLength,
            "Seek position " + pos + " exceeds the length of the file " + this.fileLength);
    if (pos == this.position) {
      return;
    }
    if (pos < this.position) {
      EOF = false;
    }
    this.position = pos;
    //TODO: do we need to keep streams in sync?
    // We use only positional reads on these streams
    getRemoteStream().seek(pos);
    this.cacheStream = getCacheStream();
    if (this.cacheStream != null) {
      try {
        this.cacheStream.seek(pos);
      } catch(IOException e) {
        //TODO: better handling exception
        LOG.error("Cached input stream seek", e);
        this.cacheStream = null;
        this.cacheStreamCallable = null;
      }
    }
  }

  @Override
  public synchronized int available() throws IOException {
    checkIfClosed();
    return (int) remaining();
  }

  @Override
  public synchronized int read() throws IOException {
    checkIfClosed();
    int n = read(one, 0, 1);
    if (n == -1) {
      return n;
    }
    return one[0] & 0xff;
  }

  @Override
  public synchronized int read(byte[] buffer) throws IOException {
    checkIfClosed();
    return read(buffer, 0, buffer.length);
  }

  @Override
  public synchronized int read(ByteBuffer buf) throws IOException {
    checkIfClosed();
    this.stats.addTotalReadRequests(1);
    int read = read(buf, buf.position(), buf.remaining()); 
    if (read > 0) {
      this.stats.addTotalBytesRead(read);
    }
    return read;
  }

  @Override
  public synchronized int read(long position, byte[] buffer, int offset, int length) throws IOException {
    checkIfClosed();
    this.stats.addTotalReadRequests(1);
    int read = readInternal(buffer, offset, length,  position, true); 
    if (read > 0) {
      this.stats.addTotalBytesRead(read);
    }
    return read;
  }

  @Override
  public synchronized void readFully(long position, byte[] buffer) throws IOException {
    checkIfClosed();
    readFully(position, buffer, 0, buffer.length);
  }

  @Override
  public synchronized void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    checkIfClosed();
        
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
    throw new IOException("seekToNewSource method is not supported.");
  }
  
  /*** End of Public API **/
  
  private long remaining() {
    return EOF ? 0 : this.fileLength - position;
  }
  
  /**
   * TODO: this method is not a public API
   * @param buffer
   * @param offset
   * @param length
   * @return
   * @throws IOException
   */
  private int read(ByteBuffer buffer, int offset, int length) throws IOException {
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
    if (length <= bufferSize) {
      // release buffer back to the I/O pool
      ioPool.offer(bytesBuffer);
    }
    return totalBytesRead;
  }

  private int readCachedPage(byte[] bytesBuffer, int offset, int length, 
                              long position) throws IOException {
    long currentPage = position / this.pageSize;
   
    int currentPageOffset = (int) (position % this.pageSize);
    // This is the assumption which is not always correct ???
    int bytesLeftInPage = (int) (this.pageSize - currentPageOffset);
    int bytesToReadInPage = Math.min(bytesLeftInPage, length);
    byte[] key = getKey(this.baseKey, currentPage * this.pageSize, this.pageSize);
    
    int bytesRead = (int) dataPageGetRange(key,  currentPageOffset, bytesToReadInPage, bytesBuffer, offset);
    if (bytesRead > 0) {
      if (bytesRead > length) {
        // FileIOEngine can not read even key-value sizes into provided buffer
        // length < bytesBuffer.length - offset
        byte[] buf = new byte[bytesRead];
        // repeat call
        bytesRead = (int) dataPageGetRange(key, currentPageOffset, bytesToReadInPage, buf, 0);
        if (bytesRead > length) {
          throw new IOException(String.format("fatal: bytes read=%d requested=%d page offset=%d to read in page=%d\n",
            bytesRead, length, currentPageOffset, bytesToReadInPage));
        }
        //Copy back to a provided buffer
        // do not assume that item still exists - it can be deleted by GC in - between
        // in this case -1 will be returned
        if(bytesRead > 0) {
          System.arraycopy(buf, 0, bytesBuffer, offset, bytesRead);
        }
      }
      if (bytesRead > 0) {
        this.stats.addTotalReadRequestsFromDataCache(1);
        this.stats.addTotalBytesReadDataCache(bytesRead);
        return bytesRead;
      }
    }
    // on local cache miss, read from an external storage. This will always make
    // progress or throw an exception
    // This is assumption that external buffer size == page size (???)
    int size = readExternalPage(position);
    long fileOffset = position / pageSize * pageSize;
    if (size > 0) {
      dataPagePut(key, 0, key.length, this.pageBuffer, 0, size, fileOffset);
      bytesToReadInPage = Math.min(bytesToReadInPage, size - currentPageOffset);
      System.arraycopy(this.pageBuffer, currentPageOffset, bytesBuffer, offset, bytesToReadInPage);
      // Can be negative
      return bytesToReadInPage < 0? 0: bytesToReadInPage;
    }
    return 0;
  }
  
  /**
   * Page range aligned with page size
   * @param position start position
   * @param len length of the range
   * @return page range (multiples of pages covering the requested range)
   */
  private Range getPageRange(long position, int len) {
    long start = position / this.pageSize * this.pageSize;
    long end = (position + len) / this.pageSize * this.pageSize;
    if (end < position + len) {
      end += Math.min(this.pageSize, this.fileLength - end);
    }
    return new Range(start, end - start);
  }
  
  /**
   * Checks which pages from page range are in the cache
   * @param r page range
   * @return boolean array 
   */
  private boolean [] inCache(Range r) {
    int n = (int) (r.size() / this.pageSize);
    if (n * this.pageSize < r.size) {
      n++;
    }
    long start = r.start / this.pageSize * this.pageSize;
    if (start + n * this.pageSize < r.start + r.size) {
      n++;
    }
    boolean[] res = new boolean[n];
    long pos = r.start;
    for (int i = 0; i < n; i++) {
      byte[] key = getKey(this.baseKey, pos, this.pageSize);
      res[i] =  dataPageExists(key); 
      pos += this.pageSize;
    }
    return res;
  }
  
  /**
   * Checks which pages from page range are in the I/O buffer
   * @param r page range
   * @return boolean array 
   */
  private boolean[] inBuffer(Range r) {
    int n = (int) (r.size() / this.pageSize);
    if (n * this.pageSize < r.size) {
      n += 1;
    }
    long start = r.start / this.pageSize * this.pageSize;
    if (start + n * this.pageSize < r.start + r.size) {
      n++;
    }
    boolean[] res = new boolean[n];
    if (this.bufferEndOffset <= r.start || this.bufferStartOffset >= r.start + r.size) {
      // no intersection
      return res;
    }
    long pos = r.start;
    for (int i = 0; i < n; i++) {
      if (pos >= this.bufferStartOffset && pos < this.bufferEndOffset) {
        res[i] = true;
      }
      pos += this.pageSize;
    }
    return res;
  }
  
  private boolean[] union(boolean[] b1, boolean[] b2) {
    boolean[] res = new boolean[b1.length];
    for (int i = 0 ; i < b1.length; i++) {
      res[i] = b1[i] || b2[i]; 
    }
    return res;
  }
  
  /**
   * B2 - B1
   * @param b1 set
   * @param b2 set
   * @return set difference
   */
  private boolean[] diff(boolean[] b1, boolean[] b2) {
    boolean[] res = new boolean[b1.length];
    for (int i = 0 ; i < b1.length; i++) {
      res[i] = !b1[i] && b2[i]; 
    }
    return res;
  }
  
  /**
   * Counts number of 'true' in boolean array
   * @param b boolean array
   * @return number of true elements
   */
  private int count(boolean[] b) {
    int n = 0;
    for (int i = 0; i < b.length; i++) {
      n += b[i]? 1: 0;
    }
    return n;
  }
    
  /******************************
   * 
   * Data cache API access
   * @throws IOException 
   *****************************/
  
  private long dataPageGetRange(byte[] key, int rangeStart, int rangeSize, byte[] buffer, int bufferOffset) 
      throws IOException {
      return cache.getRange(key, 0, key.length, rangeStart, rangeSize, true, buffer, bufferOffset);
  }
  
  private boolean dataPageExists(byte[] key) {
    return cache.exists(key);
  }

  private boolean dataPagePut(byte[] key, int keyOffset, int keySize, 
      byte[] value, int valueOffset, int valueSize, long fileOffset /* to detect scan*/)
    throws IOException
  {
    if (!cacheOnRead) {
      return false;
    }
    
    if (this.sd != null) {
      long curOffset = sd.current();
      if (curOffset != fileOffset) {
        boolean scanDetected = sd.record(fileOffset);
        if (scanDetected) {
          // Disabling this until SD issue gets its resolution: https://github.com/VladRodionov/sidecar/issues/89
          this.cacheOnRead = false;
          this.stats.addTotalScansDetected(1);
          //*DEBUG*/ LOG.error("SCAN detected {} set cache=false", path.getName());
          return false;
        }
      }
    }
    synchronized (cache) {
      // TODO: use future putIfAbsent API
      if (cache.exists(key, keyOffset, keySize)) {
        // data pages in the cache are unique because of the key naming scheme:
        // MD5(file-path + modification time + file offset)
        // so if we have the same key we have the same data page
        // no need to insert it
        // This can happen when multiple clients access the same data file
        // Exists API is cheap. Its less than 1 microsecond on average
        return false;
      }
      // Put call is cheap as well - it stores data in in-memory buffer
      // when memory buffer becomes full it is submitted asynchronously 
      // for file save operation
      return cache.put(key, keyOffset, keySize, value, valueOffset, valueSize, 0L);
    }
  }
  
  /*****************************/
  
  private int readFromPrefetchBuffer(byte[] bytesBuffer, int offset, int length,
      long position) {
    //hit or partially hit the in stream buffer
    if (position >= this.bufferStartOffset && position < this.bufferEndOffset) {
      int lengthToReadFromBuffer = (int) Math.min(length,
          this.bufferEndOffset - position);
      System.arraycopy(buffer, (int) (position - this.bufferStartOffset),
          bytesBuffer, offset, lengthToReadFromBuffer);
      this.stats.addTotalBytesReadPrefetch(lengthToReadFromBuffer);
      this.stats.addTotalReadRequestsFromPrefetch(1);
      return lengthToReadFromBuffer;
    }
    return -1;
  }
  
  private void cacheDataFromPrefetchBuffer(Range pageRange) throws IOException {
    if (!cacheOnRead) {
      return;
    }
    for(long pos = pageRange.start; pos < pageRange.start + pageRange.size; pos+= pageSize) {
      int size = (int) Math.min(pageSize, pageRange.start + pageRange.size - pos);
      if (insidePrefetchBuffer(pos, size)) {
        byte[] key = getKey(this.baseKey, pos, this.pageSize);
        long fileOffset = pos / pageSize * pageSize;
        if (!dataPageExists(key)) {
          dataPagePut(key, 0, key.length, this.buffer, (int)(pos - this.bufferStartOffset), 
            size, fileOffset);
        }
      }
    }
  }
  
  private boolean insidePrefetchBuffer(long pos, int size) {
    return pos >= bufferStartOffset && (pos + size < bufferEndOffset);
  }

  private int readFromCache(byte[] bytesBuffer, int offset, int length, long position,
      boolean isPositionedRead, Range pageRange, boolean[] in_cache) throws IOException {
    boolean fullCache = count(in_cache) == in_cache.length;
    if (fullCache) {
      // Basically reads data from the cache, but in case if any page is missing it will be
      // loaded from other sources (write cache or remote FS)
      // this can happen sometimes (rarely)
      return fullyReadFromCache(bytesBuffer, offset, length, position, isPositionedRead);
    }
    // Some pages (or all of them) are missing
    boolean[] in_buffer = inBuffer(pageRange);
    boolean[] union = union(in_cache, in_buffer);
    fullCache = count(union) == union.length;
    if (fullCache) {
      // Rest pages are in the buffer - we have to cache them
      boolean[] diff = diff(in_cache, in_buffer);
      long pos = pageRange.start;
      for (int i = 0; i < diff.length; i++) {
        if (diff[i]) {
          byte[] key = getKey(this.baseKey, pos, this.pageSize);
          int size = (int) Math.min(this.pageSize, this.bufferEndOffset - pos);
          long fileOffset = pos / pageSize * pageSize;
          dataPagePut(key, 0, key.length, this.buffer, (int) (pos - this.bufferStartOffset), size,
            fileOffset);
        }
        pos += this.pageSize;
      }
      // Now we have everything in the cache
      // TODO: performance optimization
      // double read/write
      return fullyReadFromCache(bytesBuffer, offset, length, position, isPositionedRead);
    }
    return -1;
  }
  
  
  private byte[] getBuffer(int size) {
    byte[] buf = null;
    if (this.buffer.length >= size) {
      // Read ALL into I/O buffer
      buf = this.buffer;
    } else {
      int len = (int) size;
      buf = new byte[len];
      // TODO: analyze
      this.buffer = buf;
      this.bufferStartOffset = 0;
      this.bufferEndOffset = 0;
    }
    return buf;
  }
  
  /**
   * Read internal
   * @param bytesBuffer
   * @param offset
   * @param length
   * @param position
   * @param isPositionedRead
   * @return
   * @throws IOException
   */
  private int readInternal(byte[] bytesBuffer, int offset, int length, long position,
      boolean isPositionedRead) throws IOException {

    boolean toCache = cacheOnReadThread();
    boolean oldCacheOnRead = cacheOnRead;
    cacheOnRead = toCache;
    try {
      // Adjust length
      // just in case
      length = (int) Math.min(length, this.fileLength - position);
      Range pageRange = getPageRange(position, length);
      // Try prefetch buffer first
      int read = readFromPrefetchBuffer(bytesBuffer, offset, length, position);
      if (read > 0) {
        cacheDataFromPrefetchBuffer(pageRange);
        if (!isPositionedRead) {
          this.position += read;
        }
        return read;
      }
      // Now try page cache
      boolean[] in_cache = inCache(pageRange);
      read = readFromCache(bytesBuffer, offset, length, position, isPositionedRead, pageRange,
        in_cache);
      if (read > 0) {
        return read;
      }
      // Some pages are neither in the cache nor in the I/O buffer - read ALL from write cache FS
      // or from remote FS
      byte[] buf = getBuffer((int) pageRange.size);

      // For positional reads we read only what is required
      // For non-positional we do prefetching
      int toRead = !isPositionedRead ? (int) Math.min(buf.length, this.fileLength - pageRange.start)
          : (int) Math.min(pageRange.size, this.fileLength - pageRange.start);

      // TODO: handle prefetching in external sources
      // We need 'length' of data, but can read more than that
      // if read is not positioned - we can not advance position by 'toRead' bytes

      // FIXME: this is efficiently positional reads
      // in case of streaming access can be expensive
      read = readFullyFromWriteCacheFS(pageRange.start, buf, 0, toRead, isPositionedRead, length);
      if (read < 0) {
        read = readFullyFromRemoteFS(pageRange.start, buf, 0, toRead, isPositionedRead, length);
      }
      // TODO: check on read > 0?
      this.bufferStartOffset = pageRange.start;
      this.bufferEndOffset = pageRange.start + read;

      // Save to the page cache
      long pos = pageRange.start;
      for (int i = 0; i < in_cache.length; i++) {
        if (!in_cache[i]) {
          byte[] key = getKey(this.baseKey, pos, this.pageSize);
          int size = (int) Math.min(this.pageSize, this.fileLength - pos);
          long fileOffset = pos / pageSize * pageSize;
          dataPagePut(key, 0, key.length, buf, i * this.pageSize, size, fileOffset);
        }
        pos += this.pageSize;
      }
      // Adjust stream position if not positioned read
      if (!isPositionedRead) {
        this.position += length;
      }
      // Copy from buf to bytesBuffer
      int off = (int) (position - pageRange.start);
      System.arraycopy(buf, off, bytesBuffer, offset, length);
      // Update I/O buffer range
      if (buf == this.buffer) {
        this.bufferStartOffset = pageRange.start;
        this.bufferEndOffset = pageRange.start + toRead;
      }
      return length;
    } finally {
      cacheOnRead = oldCacheOnRead;
    }
  }
  
  
  /**
   * This in fact reads from read cache
   * @param bytesBuffer buffer to read data to
   * @param offset offset in the buffer
   * @param length number of bytes to read
   * @param position start position
   * @param isPositionedRead is positioned read
   * @return number of bytes read
   * @throws IOException
   */
  private int fullyReadFromCache(byte[] bytesBuffer, int offset, int length,
                           long position, boolean isPositionedRead) throws IOException {
    // Most of the time this is a read from page cache
    checkArgument(length >= 0, "length should be non-negative");
    checkArgument(offset >= 0, "offset should be non-negative");
    checkArgument(position >= 0, "position should be non-negative");
    
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
      bytesRead = readCachedPage(bytesBuffer, offset + totalBytesRead,
          (int) (lengthToRead - totalBytesRead), currentPosition);
      totalBytesRead += bytesRead;
      currentPosition += bytesRead;
      if (!isPositionedRead) {
        this.position = currentPosition;
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
      this.cacheStream = this.cacheStreamCallable.call();
      return this.cacheStream;
    } catch (FileNotFoundException e) {
      return null;
    } catch(Exception e) {
      throw new IOException(e);
    }
  }

  private FSDataInputStream getRemoteStream() throws IOException {
    try {
      
      if (this.remoteStream != null) {
        return this.remoteStream;
      }
      this.remoteStream =  this.remoteStreamCallable.call();
      return this.remoteStream;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
  /**
   * Convenience method to ensure the stream is not closed.
   */
  private void checkIfClosed() {
    checkState(!closed, "Cannot operate on a closed stream");
  }

  private int readExternalPage(long position)
      throws IOException {
    long pageStart = position - (position % this.pageSize);
    int pageSize = (int) Math.min(this.pageSize, this.fileLength - pageStart);
    byte[] page = this.pageBuffer;
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
    int read = readFromWriteCacheFS(offset, buffer, bufOffset, len);
    if (read > 0) {
      return read;
    }
    return readFromRemoteFS(offset, buffer, bufOffset, len);
  }
  
  private int readFromRemoteFS(long position, byte[] buffer, int bufOffset, int len)
      throws IOException {
    FSDataInputStream is = getRemoteStream();
    int read = is.read(position, buffer, bufOffset, len);
    if (read > 0) {
      this.stats.addTotalReadRequestsFromRemote(1);
      this.stats.addTotalBytesReadRemote(read);
    }
    return read;
  }
  
  private int readFromWriteCacheFS(long position, byte[] buffer, int bufOffset, int len) {
    int read = -1;
    if (this.cacheStreamCallable == null) {
      return read;//
    }
    FSDataInputStream is = null;
    try {
      is = getCacheStream();
      if (is == null) {
        return read;
      }
      read = is.read(position, buffer, bufOffset, len);
      if (read > 0) {
        this.stats.addTotalReadRequestsFromWriteCache(1);
        this.stats.addTotalBytesReadWriteCache(read);
      }
    } catch(IOException e) {
      //TODO: better exception handling?
      // Basically we close write cache input stream for this file
      // Looks OK to me
      LOG.error("Reason: {}", e.getMessage());
      // try to close input stream
      try {is.close();} catch(IOException ee) {/* swallow */}
      this.cacheStreamCallable = null;
      this.cacheStream = null;
    }
    return -1;
  }
  
  private int readFullyFromRemoteFS(long position, byte[] buffer, int bufOffset, 
      int len, boolean isPositionedRead, int toAdvance)
      throws IOException {
    FSDataInputStream is = getRemoteStream();
    is.readFully(position, buffer, bufOffset, len);
    this.stats.addTotalReadRequestsFromRemote(1);
    this.stats.addTotalBytesReadRemote(len);
    return len;
  }
  
  private int readFullyFromWriteCacheFS(long position, byte[] buffer, int bufOffset, 
      int len, boolean isPositionedRead, int toAdvance) {
    if (this.cacheStreamCallable == null) {
      return -1;//
    }
    FSDataInputStream is = null;
    try { 
      is = getCacheStream();
      if (is == null) {
        return -1;
      }
      is.readFully(position, buffer, bufOffset, len);
      this.stats.addTotalReadRequestsFromWriteCache(1);
      this.stats.addTotalBytesReadWriteCache(len);
      return len;
    } catch(IOException e) {
      //TODO: better exception handling
      LOG.error("Reason: {}", e.getMessage());
      // try to close input stream
      try {is.close();} catch(IOException ee) {/* swallow */}
      this.cacheStreamCallable = null;
      this.cacheStream = null;
    }
    return -1;
  }
}
