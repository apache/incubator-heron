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

package org.apache.heron.metricsmgr.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.cache.Cache;

import org.apache.heron.metricsmgr.MetricsUtil;
import org.apache.heron.spi.metricsmgr.metrics.MetricsInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsRecord;
import org.apache.heron.spi.metricsmgr.sink.SinkContext;

import static java.lang.String.format;
import static org.apache.heron.metricsmgr.sink.PrometheusSink.Prometheus.sanitizeMetricName;

/**
 * A web sink that exposes and endpoint that Prometheus can scrape
 *
 * metrics are generated in a text format and separated with a newline "\n"
 * https://prometheus.io/docs/instrumenting/exposition_formats
 *
 * metrics format:
 * heron_metric{topology="topology-name",component="component-id",instance="instance-id"} value timestamp
 */
public class PrometheusSink extends AbstractWebSink {
  private static final Logger LOG = Logger.getLogger(PrometheusSink.class.getName());

  private static final String HERON_PREFIX = "heron";

  private static final String DELIMITER = "\n";

  // bolt metric
  private static final String METRIC_EXECUTE_TIME_NS = "__execute-time-ns";

  // spout metric
  private static final String METRIC_NEXT_TUPLE_COUNT = "__next-tuple-count";

  // This is the cache that is used to serve the metrics
  private Cache<String, Map<String, Double>> metricsCache;
  private List<Rule> rules = new ArrayList<Rule>();

  private String cluster;
  private String role;
  private String environment;

  public PrometheusSink() {
    super();
  }

  private enum Type {
    COUNTER,
    GAUGE,
    SUMMARY,
    HISTOGRAM,
    UNTYPED,
  }

  private static class Rule {
    public Pattern pattern;
    public String name;
    public String value;
    public Double valueFactor = 1.0;
    public String help;
    public boolean attrNameSnakeCase;
    public Type type = Type.UNTYPED;
    public ArrayList<String> labelNames;
    public ArrayList<String> labelValues;
  }

  @Override
  void initialize(Map<String, Object> configuration, SinkContext context) {
    metricsCache = createCache();

    cluster = context.getCluster();
    role = context.getRole();
    environment = context.getEnvironment();

    if (configuration.containsKey("rules")) {
      List<Map<String, Object>> configRules = (List<Map<String, Object>>)
          configuration.get("rules");
      for (Map<String, Object> ruleObject : configRules) {
        Rule rule = new Rule();
        rules.add(rule);
        if (ruleObject.containsKey("pattern")) {
          rule.pattern = Pattern.compile("^.*(?:" + (String) ruleObject.get("pattern") + ").*$");
        }
        if (ruleObject.containsKey("name")) {
          rule.name = (String) ruleObject.get("name");
        }
        if (ruleObject.containsKey("value")) {
          rule.value = String.valueOf(ruleObject.get("value"));
        }
        if (ruleObject.containsKey("valueFactor")) {
          String valueFactor = String.valueOf(ruleObject.get("valueFactor"));
          try {
            rule.valueFactor = Double.valueOf(valueFactor);
          } catch (NumberFormatException e) {
            // use default value
          }
        }
        if (ruleObject.containsKey("attrNameSnakeCase")) {
          rule.attrNameSnakeCase = (Boolean) ruleObject.get("attrNameSnakeCase");
        }
        if (ruleObject.containsKey("type")) {
          rule.type = Type.valueOf((String) ruleObject.get("type"));
        }
        if (ruleObject.containsKey("help")) {
          rule.help = (String) ruleObject.get("help");
        }
        if (ruleObject.containsKey("labels")) {
          TreeMap labels = new TreeMap((Map<String, Object>) ruleObject.get("labels"));
          rule.labelNames = new ArrayList<String>();
          rule.labelValues = new ArrayList<String>();
          for (Map.Entry<String, Object> entry : (Set<Map.Entry<String, Object>>) labels
              .entrySet()) {
            rule.labelNames.add(entry.getKey());
            rule.labelValues.add((String) entry.getValue());
          }
        }

        // Validation.
        if ((rule.labelNames != null || rule.help != null) && rule.name == null) {
          throw new IllegalArgumentException("Must provide name, if help or labels are given: "
              + ruleObject);
        }
        if (rule.name != null && rule.pattern == null) {
          throw new IllegalArgumentException("Must provide pattern, if name is given: "
              + ruleObject);
        }
      }
    } else {
      // Default to a single default rule.
      rules.add(new Rule());
    }
  }

  @Override
  byte[] generateResponse() throws IOException {
    metricsCache.cleanUp();
    final Map<String, Map<String, Double>> metrics = metricsCache.asMap();
    final StringBuilder sb = new StringBuilder();

    metrics.forEach((String source, Map<String, Double> sourceMetrics) -> {
      // Map the labels.
      final Map<String, String> labelKV = new TreeMap<String, String>();

      String[] sources = source.split("/");
      String topology = sources[0];
      String component = sources[1];
      String instance = sources[2];

      final boolean componentIsStreamManger = component.contains("stmgr");
      final String componentType = getComponentType(sourceMetrics);

      String c = this.cluster;
      String r = this.role;
      String e = this.environment;
      final String clusterRoleEnv = hasClusterRoleEnvironment(c, r, e)
          ? String.format("%s/%s/%s", c, r, e) : null;

      labelKV.put("topology", topology);
      labelKV.put("component", component);
      labelKV.put("instance_id", instance);

      if (clusterRoleEnv != null) {
        labelKV.put("cluster_role_env", clusterRoleEnv);
      }

      if (componentType != null) {
        labelKV.put("component_type", componentType);
      }

      sourceMetrics.forEach((String metric, Double value) -> {

        // some stream manager metrics in heron contain a instance id as part of the metric name
        // this should be a label when exported to prometheus.
        // Example: __connection_buffer_by_instanceid/container_1_word_5/packets or
        // __time_spent_back_pressure_by_compid/container_1_exclaim1_1
        // TODO convert to small classes for less string manipulation
        final String metricName;
        if (componentIsStreamManger) {
          final boolean metricHasInstanceId = metric.contains("_by_");
          final String[] metricParts = metric.split("/");
          if (metricHasInstanceId && metricParts.length == 3) {
            metricName = splitTargetInstance(metricParts[0], metricParts[2], labelKV);
            labelKV.put("metric_instance_id", metricParts[1]);
          } else if (metricHasInstanceId && metricParts.length == 2) {
            metricName = splitTargetInstance(metricParts[0], null, labelKV);
            labelKV.put("metric_instance_id", metricParts[1]);
          } else if (metricParts.length == 2) {
            metricName = splitTargetInstance(metricParts[0], metricParts[1], labelKV);
          } else {
            metricName = splitTargetInstance(metric, null, labelKV);
          }
        } else {
          final AtomicReference<String> name = new AtomicReference<>(sanitizeMetricName(metric));
          rules.forEach(rule -> {
            String ruleName = name.get();
            Matcher matcher = null;
            if (rule.pattern != null) {
              matcher = rule.pattern.matcher(metric);
              if (!matcher.matches()) {
                return;
              }
            }

            // If there's no name provided, use default export format.
            if (rule.name == null || rule.name.isEmpty()) {
              // nothing
            } else {
              // Matcher is set below here due to validation in the constructor.
              ruleName = sanitizeMetricName(matcher.replaceAll(rule.name));
              if (ruleName.isEmpty()) {
                return;
              }
            }
            if (rule.attrNameSnakeCase) {
              name.set(toSnakeAndLowerCase(ruleName));
            } else {
              name.set(ruleName.toLowerCase());
            }
            if (rule.labelNames != null) {
              for (int i = 0; i < rule.labelNames.size(); i++) {
                final String unsafeLabelName = rule.labelNames.get(i);
                final String labelValReplacement = rule.labelValues.get(i);
                String labelName = sanitizeMetricName(matcher.replaceAll(unsafeLabelName));
                String labelValue = matcher.replaceAll(labelValReplacement);
                labelName = labelName.toLowerCase();
                if (!labelName.isEmpty() && !labelValue.isEmpty()) {
                  labelKV.put(labelName, labelValue);
                }
              }
            }
          });
          metricName = name.get();
        }

        // TODO Type, Help
        String exportedMetricName = format("%s_%s", HERON_PREFIX,
            metricName
                .replace("__", "")
                .toLowerCase());
        sb.append(sanitizeMetricName(exportedMetricName))
            .append("{");
        final AtomicBoolean isFirst = new AtomicBoolean(true);
        labelKV.forEach((k, v) -> {
          // Add labels
          if (!isFirst.get()) {
            sb.append(',');
          }
          sb.append(format("%s=\"%s\"", k, v));
          isFirst.set(false);
        });
        sb.append("} ")
            .append(Prometheus.doubleToGoString(value))
            .append(" ").append(currentTimeMillis())
            .append(DELIMITER);
      });
    });

    return sb.toString().getBytes();
  }

  private static final Pattern SPLIT_TARGET = Pattern.compile("__(?<name>\\w+)"
      + "_(?<target>(?<instance>\\w+)-\\d+)");
  private static final Pattern DIGIT = Pattern.compile("[0-9]+");

  private String splitTargetInstance(String part1, String part2, Map<String, String> labelKV) {
    if (part2 != null) {
      if (DIGIT.matcher(part2).matches()) {
        labelKV.put("metric_instance_id", part2);
        return part1;
      }
      final Matcher m = SPLIT_TARGET.matcher(part1);
      if (m.matches()) {
        labelKV.put("metric_instance_id", m.group("target"));
        return String.format("%s_%s_%s", m.group("name"), m.group("instance"), part2);
      }
      return String.format("%s_%s", part1, part2);
    }
    return part1;
  }

  static String toSnakeAndLowerCase(String attrName) {
    if (attrName == null || attrName.isEmpty()) {
      return attrName;
    }
    char firstChar = attrName.subSequence(0, 1).charAt(0);
    boolean prevCharIsUpperCaseOrUnderscore = Character.isUpperCase(firstChar) || firstChar == '_';
    StringBuilder resultBuilder = new StringBuilder(attrName.length())
        .append(Character.toLowerCase(firstChar));
    for (char attrChar : attrName.substring(1).toCharArray()) {
      boolean charIsUpperCase = Character.isUpperCase(attrChar);
      if (!prevCharIsUpperCaseOrUnderscore && charIsUpperCase) {
        resultBuilder.append("_");
      }
      resultBuilder.append(Character.toLowerCase(attrChar));
      prevCharIsUpperCaseOrUnderscore = charIsUpperCase || attrChar == '_';
    }
    return resultBuilder.toString();
  }

  @Override
  public void processRecord(MetricsRecord record) {
    final String[] sources = MetricsUtil.splitRecordSource(record);

    if (sources.length > 2) {
      final String source = String.format("%s/%s/%s", getTopologyName(), sources[1], sources[2]);

      Map<String, Double> sourceCache = metricsCache.getIfPresent(source);
      if (sourceCache == null) {
        final Cache<String, Double> newSourceCache = createCache();
        sourceCache = newSourceCache.asMap();
      }

      sourceCache.putAll(processMetrics(record.getMetrics()));
      metricsCache.put(source, sourceCache);
    } else {
      LOG.log(Level.SEVERE, "Unexpected metrics source: " + record.getSource());
    }
  }

  Cache<String, Map<String, Double>> getMetricsCache() {
    return metricsCache;
  }

  long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  static boolean hasClusterRoleEnvironment(String c, String r, String e) {
    return isNotEmpty(c) && isNotEmpty(r) && isNotEmpty(e);
  }

  static boolean isNotEmpty(String string) {
    return string != null && !string.isEmpty();
  }

  static String getComponentType(Map<String, Double> sourceMetrics) {
    for (String metric : sourceMetrics.keySet()) {
      if (metric.contains(METRIC_EXECUTE_TIME_NS)) {
        return "bolt";
      }
    }

    if (sourceMetrics.containsKey(METRIC_NEXT_TUPLE_COUNT)) {
      return "spout";
    }
    return null;
  }

  static Map<String, Double> processMetrics(Iterable<MetricsInfo> metrics) {
    Map<String, Double> map = new HashMap<>();
    for (MetricsInfo r : metrics) {
      try {
        map.put(r.getName(), Double.valueOf(r.getValue()));
      } catch (NumberFormatException ne) {
        LOG.log(Level.SEVERE, "Could not parse metric, Name: "
            + r.getName() + " Value: " + r.getValue(), ne);
      }
    }

    return map;
  }

  // code taken from prometheus java_client repo
  static final class Prometheus {
    private static final Pattern METRIC_NAME_RE = Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*");
    private static final Pattern METRIC_LABEL_NAME_RE = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");
    private static final Pattern RESERVED_METRIC_LABEL_NAME_RE = Pattern.compile("__.*");

    /**
     * Throw an exception if the metric name is invalid.
     */
    static void checkMetricName(String name) {
      if (!METRIC_NAME_RE.matcher(name).matches()) {
        throw new IllegalArgumentException("Invalid metric name: " + name);
      }
    }

    private static final Pattern SANITIZE_PREFIX_PATTERN = Pattern.compile("^[^a-zA-Z_]");
    private static final Pattern SANITIZE_BODY_PATTERN = Pattern.compile("[^a-zA-Z0-9_]");

    /**
     * Sanitize metric name
     */
    static String sanitizeMetricName(String metricName) {
      return SANITIZE_BODY_PATTERN.matcher(
          SANITIZE_PREFIX_PATTERN.matcher(metricName).replaceFirst("_")
      ).replaceAll("_");
    }

    /**
     * Throw an exception if the metric label name is invalid.
     */
    static void checkMetricLabelName(String name) {
      if (!METRIC_LABEL_NAME_RE.matcher(name).matches()) {
        throw new IllegalArgumentException("Invalid metric label name: " + name);
      }
      if (RESERVED_METRIC_LABEL_NAME_RE.matcher(name).matches()) {
        throw new IllegalArgumentException(
            "Invalid metric label name, reserved for internal use: " + name);
      }
    }

    /**
     * Convert a double to its string representation in Go.
     */
    static String doubleToGoString(double d) {
      if (d == Double.POSITIVE_INFINITY) {
        return "+Inf";
      }
      if (d == Double.NEGATIVE_INFINITY) {
        return "-Inf";
      }
      if (Double.isNaN(d)) {
        return "NaN";
      }
      return Double.toString(d);
    }

    private Prometheus() {
    }
  }
}
