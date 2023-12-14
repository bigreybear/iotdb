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

import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

class ArtIterator implements Iterator<Pair<byte[], Object>> {
  private Deque<Node> elemStack = new ArrayDeque<Node>();
  private Deque<Integer> idxStack = new ArrayDeque<Integer>();

  public ArtIterator(Node root) {
    if (root != null) {
      elemStack.push(root);
      idxStack.push(0);
      maybeAdvance();
    }
  }

  @Override
  public boolean hasNext() {
    return !elemStack.isEmpty();
  }

  @Override
  public Pair<byte[], Object> next() {
    if (hasNext()) {
      Leaf leaf = (Leaf) elemStack.peek();
      byte[] key = leaf.key;
      Object value = leaf.value;

      // Mark the leaf as consumed
      idxStack.push(idxStack.pop() + 1);

      maybeAdvance();
      return new Pair<byte[], Object>(key, value);
    } else {
      throw new NoSuchElementException("end of iterator");
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  // Postcondition: if the stack is nonempty, the top of the stack must contain a leaf
  private void maybeAdvance() {
    // Pop exhausted nodes
    while (!elemStack.isEmpty() && elemStack.peek().exhausted(idxStack.peek())) {
      elemStack.pop();
      idxStack.pop();

      if (!elemStack.isEmpty()) {
        // Move on by advancing the exhausted node's parent
        idxStack.push(idxStack.pop() + 1);
      }
    }

    if (!elemStack.isEmpty()) {
      // Descend to the next leaf node element
      while (true) {
        if (elemStack.peek() instanceof Leaf) {
          // Done - reached the next element
          break;
        } else {
          // Advance to the next child of this node
          ArtNode cur = (ArtNode) elemStack.peek();
          idxStack.push(cur.nextChildAtOrAfter(idxStack.pop()));
          Node child = cur.childAt(idxStack.peek());

          // Push it onto the stack
          elemStack.push(child);
          idxStack.push(0);
        }
      }
    }
  }
}
