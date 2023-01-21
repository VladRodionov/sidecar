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
package com.carrot.sidecar.util;

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
  
  /**
   * Straw man - optimize
   * @param v long value
   * @return bytes
   */
  public static byte[] toBytes(long v) {
    return Long.toString(v).getBytes();
  }
  
  /**
   * Optimize
   * @param bytes byte array representing long
   * @return long value
   */
  public static long fromBytes(byte[] bytes) {
    return Long.parseLong(new String(bytes));
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
}
