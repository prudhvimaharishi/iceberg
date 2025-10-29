package org.apache.iceberg.spark; // <-- Note the new package

import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import scala.collection.JavaConverters;
import java.util.Map;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.AccumulableInfo;
import org.apache.spark.scheduler.SparkListener;

import org.apache.spark.scheduler.SparkListenerStageCompleted;

import org.apache.spark.scheduler.StageInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryMetricsListener extends SparkListener {

  private static Logger logger = LoggerFactory.getLogger(QueryMetricsListener.class);
  private final ConcurrentHashMap<Integer, Properties> stageProperties = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Long> scanTimeByQueryId = new ConcurrentHashMap<>();

  @Override
  public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
    int stageId = stageSubmitted.stageInfo().stageId();
    Properties props = stageSubmitted.properties();

    if (props != null) {
      stageProperties.put(stageId, props);
    }
  }

  @Override
  public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
    StageInfo info = stageCompleted.stageInfo();
    TaskMetrics metrics = info.taskMetrics();
    info.submissionTime().get();
    if (info == null) {
      return;
    }

    String queryId = stageProperties.get(stageCompleted.stageInfo().stageId()).get("jobGroup.queryName").toString();
    logger.info("Query : {}", queryId);
    if (info.accumulables() != null) {
      Collection<AccumulableInfo> updates =
          JavaConverters.mapAsJavaMap(info.accumulables()).values();
      for (AccumulableInfo accumInfo : updates) {
        if (accumInfo.name().isDefined() && accumInfo.value().isDefined()) {
          String name = accumInfo.name().get();
          Object value = accumInfo.value().get();
          logger.info("Metric {} Value {}" , name, value);
        }
      }
    }
  }

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    // Check if the event is a SQL Execution End
    if (event instanceof SparkListenerSQLExecutionEnd) {
      SparkListenerSQLExecutionEnd endEvent = (SparkListenerSQLExecutionEnd) event;
      long executionId = endEvent.executionId();
      long time = endEvent.time();

      // 'executionFailure' will be present if it failed, null otherwise (depending on Scala/Java bridge,
      // sometimes it's an Option. In raw Java it might need careful checking).
      // For simplicity here, we just note it ended.
      System.out.println("<<< Query Ended [ID: " + executionId + "] at " + time);

      // AT THIS POINT: You know the entire query is done.
      // If you were aggregating metrics in a Map<Long, Metrics> keyed by executionId,
      // now is the time to finalize and emit them.
    }
  }

}