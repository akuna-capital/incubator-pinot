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

import com.google.common.base.Preconditions;


public class Interval implements Comparable<Interval> {
  // interval with both ends inclusive [min, max]
  public final long min;
  public final long max;

  public Interval(long min, long max) {
    Preconditions.checkState(min <= max, "invalid interval [{}, {}]", min, max);
    this.min = min;
    this.max = max;
  }

  public boolean intersects(Interval that) {
    Preconditions.checkNotNull(that, "Invalid interval: null");
    if (that.max < this.min) {
      return false;
    }
    if (this.max < that.min) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(Interval that) {
    Preconditions.checkNotNull(that, "Compare to invalid interval: null");
    if (this.min < that.min) {
      return -1;
    } else if (this.min > that.min) {
      return 1;
    } else if (this.max < that.max) {
      return -1;
    } else if (this.max > that.max) {
      return 1;
    }
    else return 0;
  }

  @Override
  public int hashCode() {
    return (int)(min * 17 + max);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof Interval && that != null
        && this.min == ((Interval)that).min && this.max == ((Interval)that).max) {
      return true;
    }
    return false;
  }

  public static Interval getIntersection(Interval a, Interval b) {
    Preconditions.checkNotNull(a, "Invalid interval: null");
    Preconditions.checkNotNull(b, "Invalid interval: null");
    if (!a.intersects(b)) {
      return null;
    }
    return new Interval(Math.max(a.min, b.min), Math.min(a.max, b.max));
  }

  public static Interval getUnion(Interval a, Interval b) {
    // Can only merge two intervals if they overlaps
    Preconditions.checkNotNull(a, "Invalid interval: null");
    Preconditions.checkNotNull(b, "Invalid interval: null");
    if (!a.intersects(b)) {
      return null;
    }
    return new Interval(Math.min(a.min, b.min), Math.max(a.max, b.max));
  }

  @Override
  public String toString() {
    return "[" + min + ", " + max + "]";
  }
}
