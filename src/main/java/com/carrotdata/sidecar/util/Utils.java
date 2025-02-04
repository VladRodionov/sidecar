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
package com.carrotdata.sidecar.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.fs.Path;

import com.carrotdata.cache.util.UnsafeAccess;


public class Utils {

  public static String join(String[] args, String sep) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < args.length; i++) {
      sb.append(args[i]);
      if (i < args.length - 1) {
        sb.append(sep);
      }
    }
    return sb.toString();
  }
  
  public static String join(Object[] args, String sep) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < args.length; i++) {
      sb.append(args[i].toString());
      if (i < args.length - 1) {
        sb.append(sep);
      }
    }
    return sb.toString();
  }
  /**
   * Straw man - optimize
   * @param v long value
   * @return bytes
   */
  public static byte[] toBytes(long v) {
    byte[] buf = new byte[8];
    UnsafeAccess.putLong(buf, 0, v);
    return buf;
  }
  
  /**
   * Checks arguments
   * @param arg boolean value
   * @param message message 
   */
  public static void checkArgument(boolean arg, String message) {
    if (arg == false) {
      throw new IllegalArgumentException(message);
    }
  }
  
  /**
   * Checks state
   * @param value boolean value
   * @param message message 
   */
  public static void checkState(boolean value, String message) {
    if (value == false) {
      throw new IllegalStateException(message);
    }
  }
  
  /**
   * TODO: performance testing
   * @param arr
   * @return 16 bytes hash array
   */
  public static byte[] hashCrypto(byte[] arr) {
      MessageDigest md = null;
      try {
        md = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
      }
      md.update(arr);
      byte[] digest = md.digest();
      return digest;
  }
  
  public static byte[] getBaseKey(Path path, long modificationTime)  {
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
    }
    String skey = path.toString() + Path.SEPARATOR + modificationTime;
    md.update(skey.getBytes());
    byte[] digest = md.digest();
    byte[] baseKey = new byte[digest.length + 8];
    System.arraycopy(digest, 0, baseKey, 0, digest.length);
    return baseKey;
  }
  
  public static byte[] getKey(byte[] baseKey, long offset, long dataPageSize) {
    int size = baseKey.length;
    offset = offset / dataPageSize * dataPageSize;
    for (int i = 0; i < 8; i++) {
      int rem = (int) (offset % 256);
      baseKey[size - i - 1] = (byte) rem;
      offset /= 256;
    }
    return baseKey;
  }

}
