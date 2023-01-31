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

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

public class VirtualFileInputStream extends InputStream implements 
  Seekable, PositionedReadable {
  
  long fileLength;
  boolean closed;
  long pos;
  public VirtualFileInputStream(long fileLength) throws IOException {
    this.fileLength = fileLength;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
    if (position < 0 || position >= fileLength) {
      throw new IOException(String.format("Illegal argument %d", position));
    }
    return fillBuffer(position, buffer, offset, length);
  }

  private int fillBuffer(final long position, final byte[] buffer, final int offset, int length) {
    length = (int) Math.min(length,  fileLength - position);
    
    for(int i = offset; i < offset + length; i++) {
      buffer[i] = (byte)((i + position) % 256);
    }
    return length;
  }
  
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
    if (position < 0 || position >= fileLength) {
      throw new IOException(String.format("Illegal argument %d", position));
    }
    length = (int) Math.min(length, fileLength - position);
    int totalRead = 0;
    while(totalRead < length) {
      int read = fillBuffer(position, buffer, offset + totalRead, length - totalRead);
      totalRead += read;
      position += read;
    }
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  @Override
  public void seek(long pos) throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
    if (pos >= fileLength) {
      throw new IOException("Invalid position " + pos);
    }
    this.pos = pos;
  }

  @Override
  public long getPos() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
    return this.pos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
    return false;
  }

  @Override
  public int read() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
    return (byte) (this.pos++ % 256);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
    closed = true;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int read = read(pos, b, off, len);
    if (read == -1) {
      return -1;
    }
    pos += read;
    return read;
  }
  
}
