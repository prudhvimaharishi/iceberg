package org.apache.iceberg.spark; // <-- Note the new package

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.execution.FileSourceScanExec;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.adaptive.QueryStageExec;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.immutable.Map;

public class QueryMetricsListener implements QueryExecutionListener {

  private static Logger logger = LoggerFactory.getLogger(QueryMetricsListener.class);

  /**
   * Corrected version: Recursively traverses the plan (including AQE stages)
   * and sums the 'scanTime' metric from BatchScanExec nodes.
   *
   * @param plan The SparkPlan to inspect
   * @return Total scan time in nanoseconds
   */
  public long getTotalScanTimeNs(SparkPlan plan) {
    long totalScanTime = 0L;

    // --- AQE HANDLING ---
    // 1. If this is the main AQE plan, traverse its *final* executed plan
    if (plan instanceof AdaptiveSparkPlanExec) {
      SparkPlan finalPlan = ((AdaptiveSparkPlanExec) plan).finalPhysicalPlan();
      totalScanTime += getTotalScanTimeNs(finalPlan);
    }
    // 2. If this is a Query Stage (like ShuffleQueryStage),
    //    traverse the plan *inside* that stage
    else if (plan instanceof QueryStageExec) {
      SparkPlan stagePlan = ((QueryStageExec) plan).plan();
      totalScanTime += getTotalScanTimeNs(stagePlan);
    }
    // --- END AQE HANDLING ---


    // --- METRIC CHECK ---
    Option<SQLMetric> metricOption = null;

    // Your plan shows "BatchScan", so this is the only one we need
    if (plan instanceof BatchScanExec) {
      metricOption = ((BatchScanExec) plan).metrics().get("scanTime");
    }

    // If the 'scanTime' metric exists on this node, add its value
    if (metricOption != null && metricOption.isDefined()) {
      totalScanTime += metricOption.get().value();
    }

    // --- RECURSE INTO CHILDREN ---
    Iterator<SparkPlan> children = plan.children().iterator();
    while (children.hasNext()) {
      totalScanTime += getTotalScanTimeNs(children.next());
    }

    return totalScanTime;
  }



  @Override
  public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
    getTotalScanTimeNs(qe.executedPlan());


    System.out.println("--- [Listener] Query Succeeded: " + funcName + " (duration: " + durationNs + " ns) ---");

    SparkPlan executedPlan = qe.executedPlan();
    SparkPlan finalPlan = executedPlan; // This will be our starting point

    if (executedPlan instanceof AdaptiveSparkPlanExec) {
      System.out.println("[Listener] Found AdaptiveSparkPlan, getting final plan...");
      finalPlan = ((AdaptiveSparkPlanExec) executedPlan).finalPhysicalPlan();
    } else {
      System.out.println("[Listener] Processing non-Adaptive plan.");
    }

    // Now, call your processing function on the *correct* final plan
    processSparkPlan(finalPlan, 0);
    System.out.println("--- [Listener] End of Plan for: " + funcName + " ---");
    processSparkPlan(finalPlan, 0);
  }

//  @Override
//  public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
//    // --- NEW: Capture the query identifiers ---
//    long executionId = qe.id();
//    String logicalPlan = qe.logical().toString();
//
//    // --- NEW: Access the Job Group properties ---
//    Properties localProperties = qe.sparkSession().sparkContext().getLocalProperties();
//    String jobGroupId = localProperties.getProperty("spark.job.groupId", "N/A");
//    String jobDescription = localProperties.getProperty("spark.job.description", "{}"); // Default to empty JSON
//
//    // --- NEW: Read the custom SQL configuration key ---
//    String configKey = "spark.sql.custom.query.info";
//    // The getConf method with a default value is not available on all Spark versions,
//    // so we use a try-catch block for robustness.
//    String queryInfoJson = "{}";
//    String acceleratorEnabled = "";
//    try {
//      queryInfoJson = qe.sparkSession().conf().get(configKey);
//      acceleratorEnabled = qe.sparkSession().conf().get("spark.sql.catalog.hadoop_prod.gcs.analytics-core.enabled");
//    } catch (java.util.NoSuchElementException e) {
//      // Key was not set for this query, which is fine.
//    }
//
//    // --- Parse the JSON description to get your custom properties ---
//    String queryId = "N/A";
//    String dbName = "N/A";
//    String featureName = "N/A";
//    String featureCharacterstics = "N/A";
//    try {
//      JSONObject json = new JSONObject(queryInfoJson);
//      queryId = json.optString("query.id", "N/A");
//      dbName = json.optString("database.name", "N/A");
//      featureName = json.optString("feature.name", "N/A");
//      featureCharacterstics = json.optString("featureCharacteristics.name", "N/A");
//    } catch (Exception e) {
//      System.err.println("Could not parse query info JSON: " + queryInfoJson);
//    }
//
//    logger.info("Scan time {} of Query {} for database {} of feature {} and featureChars {} with AnalyticsCore as {}", durationNs / 1_000_000, queryId, dbName, featureName, featureCharacterstics, acceleratorEnabled);
////    inputStreamMetrics.recordScanTimeMs(durationNs/1_000_000,
////        Attributes.of(AttributeKey.stringKey("QUERY"), queryId,
////            AttributeKey.stringKey("DATABASE"), dbName,
////            AttributeKey.stringKey("ACCELERATOR_ENABLED"), acceleratorEnabled));
//  }

  @Override
  public void onFailure(String funcName, QueryExecution qe, Exception exception) {
    System.err.println("Query failed: " + funcName);
    exception.printStackTrace();
  }

  private void processSparkPlan(SparkPlan plan, int indent) {
    String indentStr = new String(new char[indent]).replace("\0", "  ");
    System.out.println(indentStr + "-> Node: " + plan.nodeName());

    Map<String, SQLMetric> metrics = plan.metrics();
    if (metrics != null && !metrics.isEmpty()) {
      Iterator<String> keys = metrics.keySet().iterator();
      while (keys.hasNext()) {
        String key = keys.next();
        metrics.get(key).foreach(metric -> {
          logger.info("{}   - {}: {}",indentStr , key, metric.value());
          return null;
        });
      }
    }

    Iterator<SparkPlan> children = plan.children().iterator();
    while (children.hasNext()) {
      processSparkPlan(children.next(), indent + 1);
    }
  }
}