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
package org.apache.pinot.broker.routing.segmentpruner;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.routing.segmentpruner.intervalST.Interval;
import org.apache.pinot.broker.routing.segmentpruner.intervalST.IntervalST;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code TimeRangeSegmentPruner} prunes segments based on their time column start & end time metadata stored in ZK. The pruner
 * supports queries with filter (or nested filter) of EQUALITY and RANGE predicates.
 */
public class TimeRangeSegmentPruner implements SegmentPruner {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeRangeSegmentPruner.class);
  private static final long MAX_END_TIME = Long.MAX_VALUE;
  private static final long MIN_START_TIME = 0;

  private final String _tableNameWithType;
  private final ZkHelixPropertyStore _propertyStore;
  private final String _segmentZKMetadataPathPrefix;
  private final String _timeColumn;
  private final TimeUnit _timeUnit;

  private volatile IntervalST _timeRangeMSToSegmentSearchTree;

  public TimeRangeSegmentPruner(TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableNameWithType = tableConfig.getTableName();
    _propertyStore = propertyStore;
    _segmentZKMetadataPathPrefix = ZKMetadataProvider.constructPropertyStorePathForResource(_tableNameWithType) + "/";
    _timeColumn = tableConfig.getValidationConfig().getTimeColumnName();
    Preconditions
        .checkNotNull(_timeColumn, "Time column must be configured in table config for table: %s", _tableNameWithType);

    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
    Preconditions.checkState(schema != null, "Failed to find schema for table: %s", _tableNameWithType);
    DateTimeFieldSpec dateTimeSpec = schema.getSpecForTimeColumn(_timeColumn);
    Preconditions.checkNotNull(dateTimeSpec, "Field spec must be specified in schema for time column: %s of table: %s",
        _timeColumn, _tableNameWithType);
    DateTimeFormatSpec formatSpec = new DateTimeFormatSpec(dateTimeSpec.getFormat());
    _timeUnit = formatSpec.getColumnUnit();
    Preconditions
        .checkNotNull(_timeUnit, "Time unit must be configured in the field spec for time column: %s of table: %s",
            _timeColumn, _tableNameWithType);
  }

  @Override
  public void init(ExternalView externalView, IdealState idealState, Set<String> onlineSegments) {
    onExternalViewChange(externalView, idealState, onlineSegments);
  }

  @Override
  public void onExternalViewChange(ExternalView externalView, IdealState idealState, Set<String> onlineSegments) {
    updateTimeRangeMSToSegmentSearchTree(onlineSegments);
  }

  private void updateTimeRangeMSToSegmentSearchTree(Set<String> onlineSegments) {
    List<String> segments = new ArrayList<>(onlineSegments);
    // atomic swap _timeRangeToSegmentMap for input online segments
    List<String> segmentZKMetadataPaths = new ArrayList<>();
    for (String segment: segments) {
      segmentZKMetadataPaths.add(_segmentZKMetadataPathPrefix + segment);
    }

    List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT, true);

    List<Long> startTimes = new ArrayList<>();
    List<Long> endTimes = new ArrayList<>();

    for (int i = 0; i < segments.size(); i++) {
      String segment = segments.get(i);

      long[] range = extractStartEndTimeMSFromSegmentZKMetaZNRecord(segment, znRecords.get(i));
      startTimes.add(range[0]);
      endTimes.add(range[1]);
    }

    _timeRangeMSToSegmentSearchTree = new IntervalST<String>(startTimes, endTimes, segments);
  }


  private long[] extractStartEndTimeMSFromSegmentZKMetaZNRecord(String segment, @Nullable ZNRecord znRecord) {
    long[] range = {MIN_START_TIME, MAX_END_TIME};
    // Segments without metadata or with invalid time range will be set with [min_start, max_end] and will not be pruned
    if (znRecord == null) {
      LOGGER.warn("Failed to find segment ZK metadata for segment: {}, table: {}", segment, _tableNameWithType);
      return range;
    }

    long startTime = znRecord.getLongField(CommonConstants.Segment.START_TIME, -1);
    long endTime = znRecord.getLongField(CommonConstants.Segment.END_TIME, -1);
    if (startTime < 0 || endTime < 0 || startTime > endTime) {
      LOGGER.warn("Failed to find valid end time for segment: {}, table: {}", segment, _tableNameWithType);
      return range;
    }

    TimeUnit timeUnit = znRecord.getEnumField(CommonConstants.Segment.TIME_UNIT, TimeUnit.class, TimeUnit.DAYS);
    range[0] = timeUnit.toMillis(startTime);
    range[1] = timeUnit.toMillis(endTime);
    return range;
  }

  @Override
  public void refreshSegment(String segment) {
    Set<String> segments;
    if (_timeRangeMSToSegmentSearchTree == null) {
      segments = new HashSet<>();
    } else {
      segments = _timeRangeMSToSegmentSearchTree.getAllValues();
    }
    if (!segment.contains(segment)) {
      segments.add(segment);
    }

    updateTimeRangeMSToSegmentSearchTree(segments);
  }

  /**
   * NOTE: Pruning is done by searching _timeRangeToSegmentSearchTree based on request time range and check if the results
   *       are in the input segments. By doing so we will have run time O(M * logN) (N is the # of all online segments,
   *       M is the # of qualified intersected segments).
   */
  @Override
  public Set<String> prune(BrokerRequest brokerRequest, Set<String> segments) {
    // The pruned order may be different from the input order
    if (_timeRangeMSToSegmentSearchTree == null) {
      return segments;
    }
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
    if (filterQueryTree == null) {
      return segments;
    }
    List<Interval> ranges = getFilterTimeRange(filterQueryTree);
    if (ranges == null) { // cannot prune based on time for input request
      return segments;
    }
    if (ranges.size() == 0) { // invalid query time range
      return Collections.emptySet();
    }

    Set<String> prunedSegments = new HashSet<>();
    for (Interval range : ranges) {
      Interval rangeMS = convertRangeToMS(range);
      for (Object segment : _timeRangeMSToSegmentSearchTree.searchAll(rangeMS)) {
        if (segments.contains(segment)) {
          prunedSegments.add((String) segment);
        }
      }
    }
    return new HashSet<String>(prunedSegments);
  }

  private List<Interval> getFilterTimeRange(FilterQueryTree filterQueryTree) {
    // return NUll if no time range info or cannot filter base on the info (e.g. 'SELECT * from myTable where time < 50 OR firstName = Jason')
    // return an empty list if filtering range is specified but invalid (e.g. 'SELECT * from myTable where time < 50 AND time > 100')
    switch (filterQueryTree.getOperator()) {
      case AND:
        List<List<Interval>> andRanges = new ArrayList<>();
        for (FilterQueryTree child : filterQueryTree.getChildren()) {
          List<Interval> childRanges = getFilterTimeRange(child);
          if (childRanges != null) {
            andRanges.add(childRanges);
          }
        }
        return getIntersectionSortedRanges(andRanges);
      case OR:
        List<List<Interval>> orRanges = new ArrayList<>();
        for (FilterQueryTree child : filterQueryTree.getChildren()) {
          List<Interval> childRanges = getFilterTimeRange(child);
          if (childRanges == null) {
            return null;
          } else {
            orRanges.add(childRanges);
          }
        }
        return getUnionSortedRanges(orRanges);
      case EQUALITY:
        if (filterQueryTree.getColumn().equals(_timeColumn)) {
          long timeStamp = Long.parseLong(filterQueryTree.getValue().get(0));
          return Collections.singletonList(new Interval(timeStamp, timeStamp));
        }
        return null;
      case RANGE:
        if (filterQueryTree.getColumn().equals(_timeColumn)) {
          return parseRange(filterQueryTree.getValue());
        }
        return null;
      default:
        return null;
    }
  }

  private Interval convertRangeToMS(Interval range) {
    long min = range.min == MIN_START_TIME ? MIN_START_TIME : _timeUnit.toMillis(range.min);
    long max = range.max == MAX_END_TIME ? MAX_END_TIME : _timeUnit.toMillis(range.max);
    return new Interval(min, max);
  }

  private List<Interval> getIntersectionSortedRanges(List<List<Interval>> ranges) {
    // Requires input ranges are sorted, the return ranges will be sorted
    return getIntersectionSortedRanges(ranges, 0, ranges.size());
  }

  private List<Interval> getIntersectionSortedRanges(List<List<Interval>> ranges, int start, int end) {
    if (start == end) {
      return null;
    }

    if (start + 1 == end) {
      return ranges.get(start);
    }

    int mid = start + (end - start) / 2;
    List<Interval> ranges1 = getIntersectionSortedRanges(ranges, start, mid);
    List<Interval> ranges2 = getIntersectionSortedRanges(ranges, mid, end);
    return getIntersectionOfTwoSortedRanges(ranges1, ranges2);
  }

  private List<Interval> getIntersectionOfTwoSortedRanges(List<Interval> ranges1, List<Interval> ranges2) { // sorted non-overlapping ranges
    List<Interval> res = new ArrayList<>();
    int i = 0, j = 0;
    while (i < ranges1.size() && j < ranges2.size()) {
      Interval range1 = ranges1.get(i);
      Interval range2 = ranges2.get(j);
      if (range1.intersects(range2)) {
        res.add(Interval.getIntersection(range1, range2));
      }
      if (range1.max < range2.max) {
        i++;
      } else {
        j++;
      }
    }
    return res;
  }

  private List<Interval> getUnionSortedRanges(List<List<Interval>> ranges) {
    return getUnionSortedRanges(ranges, 0, ranges.size());
  }

  private List<Interval> getUnionSortedRanges(List<List<Interval>> ranges, int start, int end) {
    if (start == end) {
      return null;
    }

    if (start + 1 == end) {
      return ranges.get(start);
    }

    int mid = start + (end - start) / 2;
    List<Interval> ranges1 = getUnionSortedRanges(ranges, start, mid);
    List<Interval> ranges2 = getUnionSortedRanges(ranges, mid, end);
    return getUnionOfTwoSortedRanges(ranges1, ranges2);
  }

  private List<Interval> getUnionOfTwoSortedRanges(List<Interval> ranges1, List<Interval> ranges2) { // sorted non-overlapping ranges
    List<Interval> res = new ArrayList<>();
    int i = 0, j = 0;

    while (i < ranges1.size() || j < ranges2.size()) {
      if (j == ranges2.size() || i < ranges1.size() && ranges1.get(i).compareTo(ranges2.get(j)) <= 0) {
        if (res.size() == 0 || !ranges1.get(i).intersects(res.get(res.size()-1))) {
          res.add(ranges1.get(i));
        } else {
          res.set(res.size()-1, Interval.getUnion(res.get(res.size()-1), (ranges1.get(i))));
        }
        i++;
      } else {
        if (res.size() == 0 || !ranges2.get(j).intersects(res.get(res.size()-1))) {
          res.add(ranges2.get(j));
        } else {
          res.set(res.size()-1, Interval.getUnion(res.get(res.size()-1), (ranges2.get(j))));
        }
        j++;
      }
    }
    return res;
  }

  private List<Interval> parseRange(List<String> rangeExpressions) {
    // Range is parsed as [min, max] with both min and max included for simplicity.
    // e.g. '(* 16311]' will be parsed as [0, 16311]
    // '(1455 16311)' will be parsed as [1456, 16310]
    Preconditions.checkState(rangeExpressions != null && rangeExpressions.size() == 1,
        "Cannot parse range expression from query: %s", rangeExpressions);
    long[] range = {MIN_START_TIME, MAX_END_TIME};
    String rangeExpression = rangeExpressions.get(0);
    String[] s = rangeExpression.substring(1, rangeExpression.length() - 1).split("\\s+");

    if (!s[0].equals("*")) {
      range[0] = Long.parseLong(s[0]);
      if (rangeExpression.startsWith("(")) {
        range[0]++;
      }
    }

    if (!s[1].equals("*")) {
      range[1] = Long.parseLong(s[1]);
      if (rangeExpression.endsWith(")")) {
        range[1]--;
      }
    }

    if (range[0] > range[1]) {
      return Collections.emptyList();
    }
    return Collections.singletonList(new Interval(range[0], range[1]));
  }
}
