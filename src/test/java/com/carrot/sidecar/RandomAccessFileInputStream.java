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
import java.io.RandomAccessFile;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

public class RandomAccessFileInputStream extends InputStream implements 
  Seekable, PositionedReadable {
  
  RandomAccessFile file;
  long fileLength;
  boolean closed;
  
  public RandomAccessFileInputStream(RandomAccessFile file) throws IOException {
    this.file = file;
    this.fileLength = file.length();
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
    if (position < 0 || position >= fileLength) {
      throw new IOException(String.format("Illegal argument %d", position));
    }
    file.seek(position);
    return file.read(buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
    if (position < 0 || position >= fileLength) {
      throw new IOException(String.format("Illegal argument %d", position));
    }
    int totalRead = 0;
    file.seek(position);
    while(totalRead < length) {
      int read = file.read(buffer, offset + totalRead, length - totalRead);
      totalRead += read;
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
    file.seek(pos);
  }

  @Override
  public long getPos() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
    return file.getFilePointer();
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
    return file.read();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
    file.close();
    closed = true;
  }
  
}
