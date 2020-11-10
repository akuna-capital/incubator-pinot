/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.broker.routing.segmentpruner.intervalST;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


// A read-only interval search tree based on AVL Tree
public class IntervalST<Value> {

  private Node<Value> root;

  public IntervalST(List<Long> mins, List<Long> maxs, List<Value> values) {
    // read only once constructed
    for (int i = 0; i < mins.size(); i++) {
      insert(mins.get(i), maxs.get(i), values.get(i));
    }
  }

  private void insert(long min, long max, Value value) {
    root = insert(root, new Interval(min, max), value);
  }

  private Node insert(Node node, Interval interval, Value value) {
    if (node == null) {
      return new Node(interval, value);
    }

    int cmp = interval.compareTo(node.interval);
    if (cmp == 0) {
      node.valueList.add(value);
      return node;
    } else if (cmp < 0) {
      node.left = insert(node.left, interval, value);
    } else {
      node.right = insert(node.right, interval, value);
    }

    fixAuxiliaryInfo(node);
    int balance = getBalance(node);
    // Balance current subtree
    if (balance < -1) {
      if (getBalance(node.right) > 0) {
        node.right = rotateRight(node.right); // Right-left case
      }
      node = rotateLeft(node);
    } else if (balance > 1) {
      if (getBalance(node.left) < 0) {
        node.left = rotateLeft(node.left); // Left-right case
      }
      node = rotateRight(node);
    }
    return node;
  }

  // Get all intervals which intersect with input range [min, max]
  public List<Value> searchAll(long min, long max) {
    return searchAll(new Interval(min, max));
  }

  public List<Value> searchAll(Interval interval) {
    List<Value> list = new ArrayList<Value>();
    searchAll(root, interval, list);
    return list;
  }

  private boolean searchAll(Node<Value> node, Interval interval, List<Value> list) {
    boolean foundRoot = false;
    boolean foundLeft = false;
    boolean foundRight = false;
    if (node == null)
      return false;
    if (interval.intersects(node.interval)) {
      list.addAll(node.valueList);
      foundRoot = true;
    }
    if (node.left != null && node.left.max >= interval.min) {
      foundLeft = searchAll(node.left, interval, list);
    }

    if (foundLeft || node.left == null || node.left.max < interval.min) {
      // If node.left.max > interval.min but cannot find intersections on left subtree,
      // then there won't be any intersections in right subtree, since the right most interval x
      // in left subtree must have x.min > interval.max. All intervals in right subtree
      // will have mins >= x.min, so there won't be any intersections.
      foundRight = searchAll(node.right, interval, list);
    }
    return foundRoot || foundLeft || foundRight;
  }

  public Set<Value> getAllValues() {
    Set<Value> values = new HashSet<>();
    getAllValues(root, values);
    return values;
  }

  private void getAllValues(Node<Value> node, Set<Value> values) {
    if (node == null) {
      return;
    }
    getAllValues(node.left, values);
    values.addAll(node.valueList);
    getAllValues(node.right, values);
  }

  private Node rotateRight(Node x) {
    Node y = x.left;
    x.left = y.right;
    y.right = x;
    fixAuxiliaryInfo(x);
    fixAuxiliaryInfo(y);
    return y;
  }

  private Node rotateLeft(Node x) {
    Node y = x.right;
    x.right = y.left;
    y.left = x;
    fixAuxiliaryInfo(x);
    fixAuxiliaryInfo(y);
    return y;
  }

  private int getBalance(Node node) {
    if (node == null) {
      return 0;
    }
    return height(node.left) - height(node.right);
  }

  private int height(Node node) {
    if (node == null) {
      return 0;
    }
    return node.height;
  }

  private long max(Node node) {
    if (node == null) {
      return Long.MIN_VALUE;
    }
    return node.max;
  }

  // fix auxiliary information
  private void fixAuxiliaryInfo(Node node) {
    if (node == null) return;
    node.height = 1 + Math.max(height(node.left), height(node.right));
    node.max = Math.max(max(node.left), max(node.right));
    node.max = Math.max(node.max, node.interval.max);
  }

  private static class Node<Value> {
    final Interval interval; // key
    List<Value> valueList;
    Node<Value> left, right;
    int height; // size of subtree rooted at this node
    long max; // max endpoint in subtree rooted at this node

    Node(Interval interval, Value value) {
      this.interval = interval;
      valueList = new ArrayList<>();
      valueList.add(value);
      height = 1;
      max = interval.max;
    }
  }
}
