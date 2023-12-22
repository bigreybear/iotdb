/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata.artree;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class Node implements Serializable {
  static final int MAX_PREFIX_LEN = 128;
  public long offset = -1L;
  public int depth = 0;

  // region Mod Methods

  /** Serialize to ByteArrayOutputStream for measuring size. */
  public abstract void serialize(OutputStream out) throws IOException;

  public static Node deserialize(ByteBuffer buffer) {
    byte t = ReadWriteIOUtils.readByte(buffer);
    switch (t) {
      case 0:
        return Leaf.deserialize(buffer);
      case 1:
      case 2:
      case 3:
        return ArtNode.deserialize(buffer, t);
      case 4:
        return ArtNode256.deserialize(buffer);
      default:
        throw new UnsupportedOperationException();
    }
  }

  public int getPartialLength() {
    throw new UnsupportedOperationException();
  }

  public byte[] getPartialKey() {
    throw new UnsupportedOperationException();
  }

  // NOTE my monitor for byte array
  public static String translator(byte[] src, int s, int l) {
    if (l < 0) {
      throw new UnsupportedOperationException();
    }

    byte[] res = new byte[l];
    for (int i = l; i > 0; i--) {
      res[l - i] = src[s + l - i];
    }
    return new String(res, StandardCharsets.UTF_8);
  }

  public abstract int getMaxDepth();

  // endregion

  public Node() {
    refcount = 0;
  }

  public Node(final Node other) {
    refcount = 0;
  }

  public abstract Node n_clone();

  public static Node n_clone(Node n) {
    if (n == null) return null;
    else return n.n_clone();
  }

  public abstract Leaf minimum();

  public static Leaf minimum(Node n) {
    if (n == null) return null;
    else return n.minimum();
  }

  public abstract boolean insert(
      ChildPtr ref, final byte[] key, Object value, int depth, boolean force_clone)
      throws UnsupportedOperationException;

  public static boolean insert(
      Node n, ChildPtr ref, final byte[] key, Object value, int depth, boolean force_clone) {
    // If we are at a NULL node, inject a leaf
    if (n == null) {
      ref.change(new Leaf(key, value));
      return true;
    } else {
      return n.insert(ref, key, value, depth, force_clone);
    }
  }

  public abstract boolean delete(ChildPtr ref, final byte[] key, int depth, boolean force_clone);

  public abstract int decrement_refcount();

  public abstract boolean exhausted(int i);

  public static boolean exhausted(Node n, int i) {
    if (n == null) return true;
    else return n.exhausted(i);
  }

  static int to_uint(byte b) {
    return ((int) b) & 0xFF;
  }

  int refcount;
}
