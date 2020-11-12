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
package org.apache.pinot.broker.routing.IntervalST;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.pinot.broker.routing.segmentpruner.intervalST.Interval;
import org.apache.pinot.broker.routing.segmentpruner.intervalST.IntervalST;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IntervalSTTest {

  @Test
  public void testIntervalST() {
    List<Long> starts = new ArrayList<>();
    List<Long> ends = new ArrayList<>();
    List<String> values = new ArrayList<>();

    Interval interval0 = new Interval(0, 1);
    Interval interval1 = new Interval(5, 7);
    Interval interval2 = new Interval(5, 7);
    Interval interval3 = new Interval(7, 9);
    Interval interval4 = new Interval(12, 14);
    Interval interval5 = new Interval(17, 17);
    Interval interval6 = new Interval(25, 28);
    Interval interval7 = new Interval(5, 15);
    Interval interval8 = new Interval(20, 30);
    Interval interval9 = new Interval(17, 23);
    Interval interval10 = new Interval(17, 23);
    Interval interval11 = new Interval(17, 23);
    Interval interval12 = new Interval(2, 18);

    String name0 = interval0.toString() + ": 0";
    String name1 = interval1.toString() + ": 1";
    String name2 = interval2.toString() + ": 2";
    String name3 = interval3.toString() + ": 3";
    String name4 = interval4.toString() + ": 4";
    String name5 = interval5.toString() + ": 5";
    String name6 = interval6.toString() + ": 6";
    String name7 = interval7.toString() + ": 7";
    String name8 = interval8.toString() + ": 8";
    String name9 = interval9.toString() + ": 9";
    String name10 = interval10.toString() + ": 10";
    String name11 = interval11.toString() + ": 11";
    String name12 = interval12.toString() + ": 12";

    starts.add(interval0.min);
    ends.add(interval0.max);
    values.add(name0);
    starts.add(interval1.min);
    ends.add(interval1.max);
    values.add(name1);
    starts.add(interval2.min);
    ends.add(interval2.max);
    values.add(name2);
    starts.add(interval3.min);
    ends.add(interval3.max);
    values.add(name3);
    starts.add(interval4.min);
    ends.add(interval4.max);
    values.add(name4);
    starts.add(interval5.min);
    ends.add(interval5.max);
    values.add(name5);
    starts.add(interval6.min);
    ends.add(interval6.max);
    values.add(name6);
    starts.add(interval7.min);
    ends.add(interval7.max);
    values.add(name7);
    starts.add(interval8.min);
    ends.add(interval8.max);
    values.add(name8);
    starts.add(interval9.min);
    ends.add(interval9.max);
    values.add(name9);
    starts.add(interval10.min);
    ends.add(interval10.max);
    values.add(name10);
    starts.add(interval11.min);
    ends.add(interval11.max);
    values.add(name11);
    starts.add(interval12.min);
    ends.add(interval12.max);
    values.add(name12);

    IntervalST<String> intervalST = new IntervalST<>(starts, ends, values);
    Assert.assertEquals(new HashSet(intervalST.searchAll(40, 40)), Collections.emptySet());
    Assert.assertEquals(new HashSet(intervalST.searchAll(0, 10)),
        new HashSet(Arrays.asList(name0, name1, name2, name3, name7, name12)));
    Assert.assertEquals(new HashSet(intervalST.searchAll(10, 20)),
        new HashSet(Arrays.asList(name4, name5, name7, name8, name9, name10, name11, name12)));
    Assert.assertEquals(new HashSet(intervalST.searchAll(20, 30)),
        new HashSet(Arrays.asList(name6, name8, name9, name10, name11)));
  }
}
