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

import java.util.Iterator;
import java.util.List;

class ArtNode4 extends ArtNode {
  public static int count;
  public static byte type = 1;

  public ArtNode4() {
    super();
    count++;
  }

  // region Mod Methods

  @Override
  public Iterator<Node> getChildren() {
    return new Iterator<Node>() {
      int i = 0;

      @Override
      public boolean hasNext() {
        return i < num_children;
      }

      @Override
      public Node next() {
        return children[i++];
      }
    };
  }

  @Override
  public byte getType() {
    return 1;
  }

  @Override
  public byte getKeyAt(int i) {
    return keys[i];
  }

  public static ArtNode4 padding(List<Byte> k, List<Node> c, String p) {
    ArtNode4 res = new ArtNode4();

    if (p != null) {
      res.partial_len = p.getBytes().length;
      System.arraycopy(p.getBytes(), 0, res.partial, 0, res.partial_len);
    }

    for (int i = 0; i < k.size(); i++) {
      res.add_child(null, k.get(i), c.get(i));
    }
    return res;
  }

  @Override
  public boolean valid(int i) {
    return i < num_children;
  }

  // endregion

  public ArtNode4(final ArtNode4 other) {
    super(other);
    System.arraycopy(other.keys, 0, keys, 0, other.num_children);
    for (int i = 0; i < other.num_children; i++) {
      children[i] = other.children[i];
      children[i].refcount++;
    }
    count++;
  }

  public ArtNode4(final ArtNode16 other) {
    this();
    assert (other.num_children <= 4);
    // ArtNode
    this.num_children = other.num_children;
    this.partial_len = other.partial_len;
    System.arraycopy(other.partial, 0, this.partial, 0, Math.min(MAX_PREFIX_LEN, this.partial_len));
    // ArtNode4 from ArtNode16
    System.arraycopy(other.keys, 0, keys, 0, this.num_children);
    for (int i = 0; i < this.num_children; i++) {
      children[i] = other.children[i];
      children[i].refcount++;
    }
  }

  @Override
  public Node n_clone() {
    return new ArtNode4(this);
  }

  @Override
  public ChildPtr find_child(byte c) {
    for (int i = 0; i < this.num_children; i++) {
      if (keys[i] == c) {
        return new ArrayChildPtr(children, i);
      }
    }
    return null;
  }

  @Override
  public Leaf minimum() {
    return Node.minimum(children[0]);
  }

  @Override
  public void add_child(ChildPtr ref, byte c, Node child) {
    assert (refcount <= 1);

    if (this.num_children < 4) {
      int idx;
      for (idx = 0; idx < this.num_children; idx++) {
        if (to_uint(c) < to_uint(keys[idx])) break;
      }

      // Shift to make room
      System.arraycopy(this.keys, idx, this.keys, idx + 1, this.num_children - idx);
      System.arraycopy(this.children, idx, this.children, idx + 1, this.num_children - idx);

      // Insert element
      this.keys[idx] = c;
      this.children[idx] = child;
      child.refcount++;
      this.num_children++;
    } else {
      // Copy the node4 into a new node16
      ArtNode16 result = new ArtNode16(this);
      // Update the parent pointer to the node16
      ref.change(result);
      // Insert the element into the node16 instead
      result.add_child(ref, c, child);
    }
  }

  @Override
  public void remove_child(ChildPtr ref, byte c) {
    assert (refcount <= 1);

    int idx;
    for (idx = 0; idx < this.num_children; idx++) {
      if (c == keys[idx]) break;
    }
    if (idx == this.num_children) return;

    assert (children[idx] instanceof Leaf);
    children[idx].decrement_refcount();

    // Shift to fill the hole
    System.arraycopy(this.keys, idx + 1, this.keys, idx, this.num_children - idx - 1);
    System.arraycopy(this.children, idx + 1, this.children, idx, this.num_children - idx - 1);
    this.num_children--;

    // Remove nodes with only a single child
    if (num_children == 1) {
      Node child = children[0];
      if (!(child instanceof Leaf)) {
        if (((ArtNode) child).refcount > 1) {
          child = child.n_clone();
        }
        ArtNode an_child = (ArtNode) child;
        // Concatenate the prefixes
        int prefix = partial_len;
        if (prefix < MAX_PREFIX_LEN) {
          partial[prefix] = keys[0];
          prefix++;
        }
        if (prefix < MAX_PREFIX_LEN) {
          int sub_prefix = Math.min(an_child.partial_len, MAX_PREFIX_LEN - prefix);
          System.arraycopy(an_child.partial, 0, partial, prefix, sub_prefix);
          prefix += sub_prefix;
        }

        // Store the prefix in the child
        System.arraycopy(partial, 0, an_child.partial, 0, Math.min(prefix, MAX_PREFIX_LEN));
        an_child.partial_len += partial_len + 1;
      }
      ref.change(child);
    }
  }

  @Override
  public boolean exhausted(int i) {
    return i >= num_children;
  }

  @Override
  public int nextChildAtOrAfter(int i) {
    return i;
  }

  @Override
  public Node childAt(int i) {
    return children[i];
  }

  @Override
  public int decrement_refcount() {
    if (--this.refcount <= 0) {
      int freed = 0;
      for (int i = 0; i < this.num_children; i++) {
        freed += children[i].decrement_refcount();
      }
      count--;
      // delete this;
      return freed + 128;
      // object size (8) + refcount (4) +
      // num_children int (4) + partial_len int (4) +
      // pointer to partial array (8) + partial array size (8+4+1*MAX_PREFIX_LEN)
      // pointer to key array (8) + key array size (8+4+1*4) +
      // pointer to children array (8) + children array size (8+4+8*4) +
      // padding (4)
    }
    return 0;
  }

  public byte[] keys = new byte[4];
  public Node[] children = new Node[4];
}
