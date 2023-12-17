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

import io.netty.buffer.ByteBuf;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

public class ArtTree extends ChildPtr implements Serializable {
  public ArtTree() {}

  public static void main(String[] args) throws IOException {
    System.out.println("hello");
    ArtTree tree = new ArtTree();
    //
    tree.insert("root.sg1.d2.v2".getBytes(StandardCharsets.UTF_8), 1L);
    tree.insert("root.sg2.d3.v3".getBytes(StandardCharsets.UTF_8), 2L);
    tree.insert("root.sg2.d4.v1".getBytes(StandardCharsets.UTF_8), 3L);
    tree.insert("root.sg2.d3.v1".getBytes(StandardCharsets.UTF_8), 4L);
    tree.insert("root.sg2.xd3.xv1".getBytes(StandardCharsets.UTF_8), 11L);
    tree.insert("root.sg5.d1.v1".getBytes(StandardCharsets.UTF_8), 5L);
    tree.insert("root.sg5.d2.v1".getBytes(StandardCharsets.UTF_8), 6L);
    tree.insert("root.sg6.d1.v1".getBytes(StandardCharsets.UTF_8), 7L);
    calculateDepth(tree);
//    System.out.println(tree);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serialize(((ArtNode)tree.root), baos);
    ReadWriteIOUtils.write(tree.root.offset, baos);
    System.out.println(baos.size());
    byte[] res = baos.toByteArray();
    traverse((ArtNode) tree.root, "");

    ArtNode r2 = (ArtNode) deserialize(ByteBuffer.wrap(res));
    traverseAfterDeserialize(r2, "");
  }

  public static Node deserialize(ByteBuffer buffer) {
    int start = buffer.capacity() - 8;
    buffer.position(start);
    buffer.position((int) ReadWriteIOUtils.readLong(buffer));
    return Node.deserialize(buffer);
  }

  public static void serialize(ArtNode node, ByteArrayOutputStream out) throws IOException {
    Node child;
    for (int i = 0; !node.exhausted(i); i++) {
      if (!node.valid(i)) {
        continue;
      }
      child = node.childAt(i);
      if (child instanceof Leaf) {
        child.offset = out.size();
        child.serialize(out);
      }
      if (child instanceof ArtNode) {
        serialize((ArtNode) child, out);
      }
      if (node.exhausted(i+1)) {
        node.offset = out.size();
        node.serialize(out);
      }
    }
  }

  public static void traverseAfterDeserialize(ArtNode node, String prefix) {
    String res = prefix + new String(node.getPartialKey(), 0, node.getPartialLength());
    Node child;
    Leaf leaf;
    String base;
    for (int i = 0; !node.exhausted(i); i++) {
      if (!node.valid(i)) {
        continue;
      }
      child = node.childAt(i);
      if (child instanceof Leaf) {
        leaf = (Leaf) child;
        base = res + (char) node.getKeyAt(i);
        base = base + new String(leaf.getPartialKey());
        System.out.println(String.format("%s,%d", base, ((Long) leaf.value)));
      } else {
        traverseAfterDeserialize((ArtNode) child, res + (char)node.getKeyAt(i));
      }
    }
  }

  public static void traverse(ArtNode node, String prefix) {
    String res = prefix + new String(node.getPartialKey(), 0, node.getPartialLength());
    Node child;
    Leaf leaf;
    String base;
    for (int i = 0; !node.exhausted(i); i++) {
      if (!node.valid(i)) {
        continue;
      }
      child = node.childAt(i);
      if (child instanceof Leaf) {
        leaf = (Leaf) child;
        base = res + (char) node.getKeyAt(i);
        base = base + Node.translator(leaf.key, leaf.depth, leaf.remain);
        System.out.println(String.format("%s,%d", base, ((Long) leaf.value)));
      } else {
        traverse((ArtNode) child, res + (char)node.getKeyAt(i));
      }
    }
  }

  public static void mark(Node node) {
    System.out.println(node);
  }

  public static void calculateDepth(ArtTree tree) {
    Node node, child;
    ArtNode anode;
    Leaf leaf;
    int deep = 0, i = 0;
    Deque<Node> stk = new ArrayDeque<>();
    stk.push(tree.root);
    String res;

    while (stk.size() != 0) {
      node = stk.pop();
      if (node instanceof ArtNode) {
        anode = (ArtNode) node;
        i = 0;
        while (!anode.exhausted(i)) {
          if (!anode.valid(i)) {
            i++;
            continue;
          }
          child = anode.childAt(i);
          child.depth = anode.depth + anode.partial_len + 1;
          if (child instanceof Leaf) {
            ((Leaf) child).remain = ((Leaf) child).key.length - child.depth;
          }
          stk.push(child);
          i++;
        }
      } else if (node instanceof Leaf) {
        res = ArtNode.translator(((Leaf) node).key, node.depth, ((Leaf) node).remain);
//        System.out.println(res);
        continue;
      } else {
        throw new UnsupportedOperationException();
      }
    }
  }

  public ArtTree(final ArtTree other) {
    root = other.root;
    num_elements = other.num_elements;
  }

  public ArtTree snapshot() {
    ArtTree b = new ArtTree();
    if (root != null) {
      b.root = Node.n_clone(root);
      b.root.refcount++;
    }
    b.num_elements = num_elements;
    return b;
  }

  @Override
  Node get() {
    return root;
  }

  @Override
  void set(Node n) {
    root = n;
  }

  public Object search(final byte[] key) {
    Node n = root;
    int prefix_len, depth = 0;
    while (n != null) {
      if (n instanceof Leaf) {
        Leaf l = (Leaf) n;
        // Check if the expanded path matches
        if (l.matches(key)) {
          return l.value;
        } else {
          return null;
        }
      } else {
        ArtNode an = (ArtNode) (n);

        // Bail if the prefix does not match
        if (an.partial_len > 0) {
          prefix_len = an.check_prefix(key, depth);
          if (prefix_len != Math.min(Node.MAX_PREFIX_LEN, an.partial_len)) {
            return null;
          }
          depth += an.partial_len;
        }

        if (depth >= key.length) return null;

        // Recursively search
        ChildPtr child = an.find_child(key[depth]);
        n = (child != null) ? child.get() : null;
        depth++;
      }
    }
    return null;
  }

  public void insert(final byte[] key, Object value) throws UnsupportedOperationException {
    if (Node.insert(root, this, key, value, 0, false)) num_elements++;
  }

  public void delete(final byte[] key) {
    if (root != null) {
      boolean child_is_leaf = root instanceof Leaf;
      boolean do_delete = root.delete(this, key, 0, false);
      if (do_delete) {
        num_elements--;
        if (child_is_leaf) {
          // The leaf to delete is the root, so we must remove it
          root = null;
        }
      }
    }
  }

  public Iterator<Pair<byte[], Object>> iterator() {
    return new ArtIterator(root);
  }

  public Iterator<Pair<byte[], Object>> prefixIterator(final byte[] prefix) {
    // Find the root node for the prefix
    Node n = root;
    int prefix_len, depth = 0;
    while (n != null) {
      if (n instanceof Leaf) {
        Leaf l = (Leaf) n;
        // Check if the expanded path matches
        if (l.prefix_matches(prefix)) {
          return new ArtIterator(l);
        } else {
          return new ArtIterator(null);
        }
      } else {
        if (depth == prefix.length) {
          // If we have reached appropriate depth, return the iterator
          if (n.minimum().prefix_matches(prefix)) {
            return new ArtIterator(n);
          } else {
            return new ArtIterator(null);
          }
        } else {
          ArtNode an = (ArtNode) (n);

          // Bail if the prefix does not match
          if (an.partial_len > 0) {
            prefix_len = an.prefix_mismatch(prefix, depth);
            if (prefix_len == 0) {
              // No match, return empty
              return new ArtIterator(null);
            } else if (depth + prefix_len == prefix.length) {
              // Prefix match, return iterator
              return new ArtIterator(n);
            } else {
              // Full match, go deeper
              depth += an.partial_len;
            }
          }

          // Recursively search
          ChildPtr child = an.find_child(prefix[depth]);
          n = (child != null) ? child.get() : null;
          depth++;
        }
      }
    }
    return new ArtIterator(null);
  }

  public long size() {
    return num_elements;
  }

  public int destroy() {
    if (root != null) {
      int result = root.decrement_refcount();
      root = null;
      return result;
    } else {
      return 0;
    }
  }

  public Node root = null;
  long num_elements = 0;
}
