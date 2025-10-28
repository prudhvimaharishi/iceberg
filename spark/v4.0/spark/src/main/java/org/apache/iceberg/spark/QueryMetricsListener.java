package org.apache.iceberg.spark; // <-- Note the new package


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueryMetricsListener implements QueryExecutionListener {

  private static Logger logger = LoggerFactory.getLogger(QueryMetricsListener.class);
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;
  private final String driverHost = "localhost"; // Your driver host
  private final String driverPort = "4041";      // Your driver port

  private static final Pattern DURATION_PATTERN = Pattern.compile("^([0-9.]+)\\s+(m|s|ms)");
  private static final Pattern WSC_NAME_PATTERN = Pattern.compile("WholeStageCodegen \\((\\d+)\\)");
  private static final Pattern TIME_PATTERN = Pattern.compile("([0-9.]+)\\s+(m|s|ms)");

  public QueryMetricsListener() {
    this.httpClient = HttpClient.newHttpClient();
    this.jsonMapper = new ObjectMapper();
  }

  @Override
  public void onSuccess(String funcName, org.apache.spark.sql.execution.QueryExecution qe, long durationNs) {
    String queryInfoJson = "{}";
    String acceleratorEnabled = "";
    try {
      queryInfoJson = qe.sparkSession().conf().get("spark.sql.custom.query.info");
      acceleratorEnabled = qe.sparkSession().conf().get("spark.sql.catalog.hadoop_prod.gcs.analytics-core.enabled");
    } catch (java.util.NoSuchElementException e) {
      // Key was not set for this query, which is fine.
    }

    String query_id = "N/A";
    String dbName = "N/A";
    String featureName = "N/A";
    String featureCharacterstics = "N/A";

    JSONObject json = new JSONObject(queryInfoJson);
    query_id = json.optString("query.id", "N/A");
    dbName = json.optString("database.name", "N/A");
    featureName = json.optString("feature.name", "N/A");
    featureCharacterstics = json.optString("featureCharacteristics.name", "N/A");

    int queryNumber = Integer.parseInt(query_id.substring(1, query_id.length() - 1));
    String queryId = String.valueOf(queryNumber-1);
    if (queryId == null) {
     queryId = "0";
    }
    String appId = qe.sparkSession().sparkContext().applicationId();
    String apiUrl = String.format("http://%s:%s/api/v1/applications/%s/sql/%s", // Use %s for the queryId string
        driverHost, driverPort, appId, queryId);
    try {
      HttpRequest request = HttpRequest.newBuilder().uri(new URI(apiUrl)).GET().build();
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 200) {
        JsonNode root = jsonMapper.readTree(response.body());
        JsonNode nodesNode = root.get("nodes");
        List<JsonNode> allNodes = new ArrayList<>();
        if (nodesNode != null && nodesNode.isArray()) {
          for (JsonNode node : nodesNode) {
            allNodes.add(node);
          }
        }
        String treeString = buildTreeFromJson(root);
        logger.info("Collected tree : {}", treeString);
        Map<String, Long> scanTimes = computeScanTimes(root);
        logger.info("Collected scan times: {}", scanTimes);


        logger.info("Scan time {} of Query {} for database {} of feature {} and featureChars {} with AnalyticsCore as {}", durationNs / 1_000_000, query_id, dbName, featureName, featureCharacterstics, acceleratorEnabled);

      } else {
        System.err.println("[NodeListener] ERROR: API request failed for query " + queryId + ". Status: " + response.statusCode());
      }

    } catch (Exception e) {
      System.err.println("[NodeListener] ERROR: Failed to fetch/parse API for query " + queryId + ": " + e.getMessage());
    }
  }

  @Override
  public void onFailure(String funcName, org.apache.spark.sql.execution.QueryExecution qe, Exception exception) {
    String queryId = qe.sparkSession().sparkContext().getLocalProperty("");
    System.err.println("[NodeListener] Query " + (queryId != null ? queryId : "?") + " failed: " + exception.getMessage());
  }

  public Map<String, Long> computeScanTimes(JsonNode root) {
    long totalMetadataTime = 0;
    long totalDataWallClockTime = 0; // Renamed for clarity

    JsonNode nodes = root.get("nodes");
    JsonNode edges = root.get("edges");

    Map<Integer, JsonNode> nodeMap = new HashMap<>();
    Map<String, Long> wscIdToDurationMap = new HashMap<>();
    Map<Integer, String> nodeToWscIdMap = new HashMap<>();
    if (nodes != null && nodes.isArray()) {
      for (JsonNode node : nodes) {
        int nodeId = node.get("nodeId").asInt();
        String nodeName = node.get("nodeName").asText();
        nodeMap.put(nodeId, node);
        Matcher wscMatcher = WSC_NAME_PATTERN.matcher(nodeName);
        if (wscMatcher.find()) {
          wscIdToDurationMap.put(wscMatcher.group(1), getWallClockDurationMetric(node, "duration"));
        }
        if (node.has("wholeStageCodegenId")) {
          nodeToWscIdMap.put(nodeId, node.get("wholeStageCodegenId").asText());
        }
      }
    }
    Map<Integer, Integer> childToParentMap = new HashMap<>();
    if (edges != null && edges.isArray()) {
      for (JsonNode edge : edges) {
        childToParentMap.put(edge.get("fromId").asInt(), edge.get("toId").asInt());
      }
    }

    Set<String> wscIdsToSum = new HashSet<>();
    for (JsonNode node : nodeMap.values()) {
      if (node.get("nodeName").asText().startsWith("BatchScan")) {
        totalMetadataTime += getSimpleNumericMetric(node, "total planning duration (ms)");
        Integer parentId = childToParentMap.get(node.get("nodeId").asInt());
        if (parentId != null) {
          String wscId = nodeToWscIdMap.get(parentId);
          if (wscId != null) {
            wscIdsToSum.add(wscId);
          }
        }
      }
    }

    for (String wscId : wscIdsToSum) {
      totalDataWallClockTime += wscIdToDurationMap.getOrDefault(wscId, 0L);
    }

    Map<String, Long> scanTimes = new HashMap<>();
    scanTimes.put("totalMetadataPlanningTimeMs", totalMetadataTime);
    scanTimes.put("totalDataScanWallClockTimeMs", totalDataWallClockTime); // Renamed for clarity
    return scanTimes;
  }

  private long getWallClockDurationMetric(JsonNode node, String metricName) {
    JsonNode metrics = node.get("metrics");
    if (metrics == null || !metrics.isArray()) return 0;

    for (JsonNode metric : metrics) {
      if (metric.get("name").asText().equals(metricName)) {
        String value = metric.get("value").asText();
        String[] lines = value.split("\n");
        if (lines.length < 1) return 0;

        String timeLine = lines[lines.length - 1].trim(); // "38.5 m (...)"

        int parenOpen = timeLine.indexOf('(');
        int parenClose = timeLine.lastIndexOf('('); // Find the *last* open paren
        if (parenOpen == -1 || parenOpen == parenClose) return 0; // No (min, med, max)

        String minMedMax = timeLine.substring(parenOpen + 1, parenClose).trim();

        String[] timeParts = minMedMax.split(",");
        if (timeParts.length < 1) return 0;

        String maxTimeStr = timeParts[timeParts.length - 1].trim(); // "5.3 s"

        Matcher matcher = TIME_PATTERN.matcher(maxTimeStr);
        if (matcher.find()) {
          try {
            double timeValue = Double.parseDouble(matcher.group(1)); // "5.3"
            String unit = matcher.group(2); // "s"

            if ("m".equals(unit)) {
              return (long) (timeValue * 60 * 1000);
            } else if ("s".equals(unit)) {
              return (long) (timeValue * 1000);
            } else if ("ms".equals(unit)) {
              return (long) timeValue;
            }
          } catch (Exception e) {
            return 0;
          }
        }
      }
    }
    return 0;
  }

  private long getSimpleNumericMetric(JsonNode node, String metricName) {
    JsonNode metrics = node.get("metrics");
    if (metrics != null && metrics.isArray()) {
      for (JsonNode metric : metrics) {
        if (metric.get("name").asText().equals(metricName)) {
          try {
            return Long.parseLong(metric.get("value").asText());
          } catch (NumberFormatException e) {
            return 0;
          }
        }
      }
    }
    return 0;
  }

  private String buildTreeFromJson(JsonNode root) {
    // ... (omitted for brevity, this is the corrected version from before) ...
    JsonNode nodes = root.get("nodes");
    JsonNode edges = root.get("edges");
    Map<Integer, JsonNode> nodeMap = new HashMap<>();
    if (nodes != null && nodes.isArray()) {
      for (JsonNode node : nodes) { nodeMap.put(node.get("nodeId").asInt(), node); }
    }
    Map<Integer, List<Integer>> parentToChildrenMap = new HashMap<>();
    Set<Integer> allChildIds = new HashSet<>();
    Set<Integer> allParentIds = new HashSet<>();
    if (edges != null && edges.isArray()) {
      for (JsonNode edge : edges) {
        int parentId = edge.get("toId").asInt(); int childId = edge.get("fromId").asInt();
        parentToChildrenMap.computeIfAbsent(parentId, k -> new ArrayList<>()).add(childId);
        allChildIds.add(childId); allParentIds.add(parentId);
      }
    }
    Integer rootId = null;
    for (Integer parentId : allParentIds) { if (!allChildIds.contains(parentId)) { rootId = parentId; break; } }
    if (rootId == null) {
      for (Integer nodeId : nodeMap.keySet()) { if (!allChildIds.contains(nodeId)) { rootId = nodeId; break; } }
    }
    if (rootId == null) { return "Error: Could not find root node in query plan."; }
    StringBuilder sb = new StringBuilder();
    buildTreeRecursive(nodeMap.get(rootId), nodeMap, parentToChildrenMap, 0, sb);
    return sb.toString();
  }

  private void buildTreeRecursive(JsonNode node, Map<Integer, JsonNode> nodeMap,
                                  Map<Integer, List<Integer>> parentToChildrenMap,
                                  int indent, StringBuilder sb) {
    if (node == null) return;

    String indentation = new String(new char[indent]).replace("\0", "  ");
    sb.append(indentation)
        .append("+- ")
        .append(node.get("nodeName").asText());

    // --- THIS IS THE CHANGE ---
    // Check if the node is tagged with a WSC ID and print it
    if (node.has("wholeStageCodegenId")) {
      String wscId = node.get("wholeStageCodegenId").asText();
      sb.append(" [WSC_ID: ").append(wscId).append("]");
    }
    // --- END CHANGE ---

    // Add the node ID
    sb.append(" (nodeId: ")
        .append(node.get("nodeId").asInt())
        .append(")\n");

    // Recurse for all children
    List<Integer> childIds = parentToChildrenMap.get(node.get("nodeId").asInt());
    if (childIds != null) {
      childIds.sort(Integer::compareTo);
      for (int childId : childIds) {
        buildTreeRecursive(nodeMap.get(childId), nodeMap, parentToChildrenMap, indent + 1, sb);
      }
    }
  }

}