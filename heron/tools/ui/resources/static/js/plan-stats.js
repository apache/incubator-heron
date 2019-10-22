/*
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

/**
 * Generate the stats rollup table.
 */
(function (global) {
  function drawStatsTable(planController, baseUrl, cluster, environ, toponame, physicalPlan, logicalPlan) {
    var NO_DATA_COLOR = "#f0f5fa";

    var table = d3.selectAll('.stat-rollup-table tbody.stats');
    var percentFormat = d3.format('.1%');

    var d3instances = d3.selectAll('#physical-plan .instance');
    var d3instancedata = d3instances.data();

    // Build the status table
    var rowData = [
      {
        name: 'Capacity Utilization',
        metricName: 'capacity',
        get: getAndRenderStats,
        tooltip: 'Average tuple execute latency multiplied by number of tuples processed divided by the time period.',
        legendDescription: 'utilization of execution capacity',
        loMedHi: [0.1, 0.5, 1],
        scaleTicks: [0, 0.25, 0.5, 0.75, 1]
      },
      {
        name: 'Failures',
        metricName: 'failures',
        get: getAndRenderStats,
        tooltip: 'Failed tuple count divided by sum of failed and executed tuples over time range.',
        legendDescription: 'failure rate',
        loMedHi: [0, 0.02, 0.05],
        scaleTicks: [0, 0.01, 0.02, 0.03, 0.04, 0.05]
      },
      {
        name: 'CPU',
        metricName: 'cpu',
        get: getAndRenderStats,
        tooltip: 'CPU seconds used per second.',
        legendDescription: 'CPUs used',
        loMedHi: [1, 1.5, 2],
        scaleTicks: [0, 0.5, 1, 1.5, 2],
        format: function (d) { return d.toFixed(2); }
      },
      {
        name: 'Memory',
        metricName: 'memory',
        get: getAndRenderStats,
        tooltip: 'Memory used divided by total available memory per process.',
        legendDescription: 'memory usage',
        loMedHi: [0.7, 0.85, 1],
        scaleTicks: [0, 0.25, 0.5, 0.75, 1],
      },
      {
        name: 'GC',
        metricName: 'gc',
        get: getAndRenderStats,
        tooltip: 'Milliseconds spent in garbage collection per minute.',
        legendDescription: 'ms in GC per minute',
        loMedHi: [1000, 1500, 2000],
        scaleTicks: [0, 500, 1000, 1500, 2000],
        format: function (d) { return d.toFixed(0); }
      },
      {
        name: 'Back Pressure',
        metricName: 'backpressure',
        get: getAndRenderStats,
        tooltip: 'Milliseconds spent in back pressure per minute.',
        legendDescription: 'ms in back pressure per minute',
        loMedHi: [3000, 12000, 30000],
        scaleTicks: [0, 7500, 15000, 22500, 30000],
        format: function (d) { return d.toFixed(0); }
      }
    ];

    var colData = [
      {name: '3 min', seconds: 3*60},
      {name: '10 min', seconds: 10*60},
      {name: '1 hour', seconds: 60*60},
      {name: '3 hours', seconds: 3*60*60}
    ];
    window.pollingMetrics = rowData;
    window.pollingPeriods = colData;

    var colorRanges = {
      // green/red
      'default': ['#1a9850', '#fdae61', '#d73027'],
      // blue/red
      'colorblind': ['#2166ac', '#f7f7f7', '#b2182b']
    };

    rowData.forEach(function (d) {
      d.format = d.format || d3.format('%');
      d.colorScale = d3.scale.linear()
          .interpolate(d3.interpolateLab)
          .domain(d.loMedHi)
          .range(colorRanges[localStorage.colors] || colorRanges.default)
          .clamp(true);
    });

    var rows = table.selectAll('tr')
        .data(rowData)
        .enter()
      .append('tr');

    rows.append('td')
        .attr('class', 'text-right')
        .html(function (d) {
          return d.name + ' <span class="glyphicon glyphicon-question-sign bs-popover text-muted" aria-hidden="true" data-toggle="popover" data-placement="top" title="' + d.name + '" data-content="' + d.tooltip + '"></span></a>';
        });

    $('.bs-popover').popover({
      trigger: 'hover',
      container: 'body'
    });

    var cols = rows.selectAll('td.time')
        .data(function (row) {
          return colData.map(function (col) {
            return {
              metric: row,
              time: col
            };
          });
        })
        .enter()
      .append('td')
        .attr('class', 'time');

    var links = cols.append('a')
      .attr('href', '#')
      .attr('class', 'strong')
      .on('click', function (d) {
        d.metric.get(d);
        d3.event.preventDefault();
        d3.event.stopPropagation();
      });

    var topLevelStatus = links.append('svg')
        .attr('class', 'status-circle')
        .attr('width', 20)
        .attr('height', 15)
      .append('circle')
        .attr('cx', 8)
        .attr('cy', 8)
        .attr('r', 7);

    links.append('span')
        .text(function (d) { return d.time.name; });

    d3.selectAll('.reset').on('click', function () {
      resetColors();
    });

    d3.selectAll('#reset-colors').on('click', function () {
      resetColors();
      d3.event.stopPropagation();
      d3.event.preventDefault();
    });

    // Start polling once a minute for all top-level stats
    function fillInTopLevelStats() {
      topLevelStatus.each(function (d) {
        d.metric.get(d, true);
      });
    }
    // If there are more than 20 logical nodes or 400 physical instances, don't request metrics automatically
    // instead wait for the user to click on them to fill in.
    setTimeout(function () {
      if (d3.selectAll('#logical-plan .node').size() <= 20 && d3.selectAll('#physical-plan .instance').size() <= 400) {
        fillInTopLevelStats();
        setInterval(fillInTopLevelStats, 60000);
      }
    }, 20);

    function getAndRenderStats(statTableCell, justFillInTopLevel) {
      var seconds = statTableCell.time.seconds;
      var durationString = statTableCell.time.name;

      if (!justFillInTopLevel) {
        drawColorKey(statTableCell);
        clearComponentColors(statTableCell);
      }
      makeMetricsQuery(statTableCell.metric, seconds / 60, function (data) {
        var max = d3.max(data, function (d) { return d.value; });
        _.pairs(_.groupBy(data, 'name')).forEach(function (pair) {
          var name = pair[0];
          var values = _.pluck(pair[1], 'value');
          var maxValue = d3.max(values);
          pair[1].forEach(function (datapoint) {
            recordInstanceMetric(datapoint.value, datapoint.id, statTableCell);
          });
          if (!justFillInTopLevel) {
            colorGraphNodes(maxValue, name, statTableCell);
            pair[1].forEach(function (datapoint) {
              colorInstances(datapoint.value, datapoint.id, statTableCell);
            });
          }
        });
        colorTopLevel(max, statTableCell);
      });
    }

    // Re-render the color key based on current metric displayed
    function drawColorKey(statTableCell) {
      var parentSelector = '#color-key';
      var tickValues = statTableCell.metric.scaleTicks;
      var gradientID = 'colorKey';
      var height = 3;
      var outerWidth = 400;
      var outerHeight = 70;
      var margin = {top: 25, left: 50, bottom: 20, right: 50};
      var width = outerWidth - margin.left - margin.right;

      var keyContainer = d3.selectAll(parentSelector);
      var svg = keyContainer.text('').append('svg')
          .attr('width', outerWidth)
          .attr('height', outerHeight)
        .append('g')
          .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');

      svg.append('text')
          .attr('y', -10)
          .attr('x', width / 2)
          .style('text-anchor', 'middle')
          .text('Instances colored by ' + statTableCell.metric.legendDescription + ' over last ' + statTableCell.time.name);

      var xScale = d3.scale.linear()
          .domain(d3.extent(tickValues))
          .range([0, width]);

      // encode the color key as a gradient that stretches across the wide rectangle
      d3.selectAll('svg.svgdefs').remove();
      var gradient = d3.select('body')
        .append('svg')
        .attr('class', 'svgdefs')
        .attr('width', 0)
        .attr('height', 0)
        .append("defs", 'defs')
        .append('linearGradient', gradientID)
          .attr("id", gradientID)
          .attr("x1", "0%")
          .attr("y1", "0%")
          .attr("x2", "100%")
          .attr("y2", "0%")
          .attr("spreadMethod", "pad");

      var valueToPercentScale = d3.scale.linear()
          .domain(d3.extent(tickValues))
          .range(["0%", "100%"]);

      gradient.selectAll('stop')
          .data(statTableCell.metric.colorScale.domain())
          .enter()
        .append("svg:stop")
          .attr("offset", valueToPercentScale)
          .attr("stop-color", statTableCell.metric.colorScale)
          .attr("stop-opacity", 1);
      svg.append('rect')
        .attr('width', width)
        .attr('height', height)
        .attr('fill', 'url(#' + gradientID + ')');

      // also label specific points on the color scale
      var colorScaleAxis = d3.svg.axis()
        .scale(xScale)
        .orient('bottom')
        .tickValues(tickValues)
        .tickFormat(statTableCell.metric.format || d3.format('%'))
        .tickSize(2);
      svg
        .append('g')
          .attr('class', 'x axis')
          .attr('transform', function() {return 'translate(' + 0 +',' + height + ')'; })
          .call(colorScaleAxis);
    }

    // Utilities
    function getLogicalComponentNames() {
      var toRequest = {};

      d3.selectAll('#logical-plan .node').each(function (d) {
        if (d.name) {
          toRequest[d.name] = true;
        }
      });

      return _.keys(toRequest);
    }

    function p90(data) {
      return d3.quantile(_.sortBy(data), 0.9);
    }

    function cleanValue(value) {
      return value === 'nan' ? 0 : +value;
    }

    function colorGraphNodes(value, name, statTableCell) {
      d3.selectAll('#logical-plan .node').filter(function (d) {
        return d.name === name;
      }).style('fill', function (d) {
        return d.color = !_.isNumber(value) || isNaN(value) ? NO_DATA_COLOR : statTableCell.metric.colorScale(value);
      });
    }

    function clearComponentColors(statTableCell) {
      d3.selectAll('#physical-plan .instance, #logical-plan .node').style('fill', function (d) {
        d.currentMetric = statTableCell;
        d.currentMetricValue = null;
        return d.color = NO_DATA_COLOR;
      });
    }

    function recordInstanceMetric(value, id, statTableCell) {
      var name = statTableCell.metric.name;
      var seconds = statTableCell.time.seconds;
      var d;
      for (var i = d3instancedata.length - 1; i >= 0; i--) {
        d = d3instancedata[i];
        if (d.id === id) {
          d.stats = d.stats || {};
          d.stats[name] = d.stats[name] || {};
          d.stats[name][seconds] = value;
        }
      }
    }

    function colorInstances(value, id, statTableCell, tooltipDetails) {
      d3.select('html').classed('no-colors', false);
      d3instances.filter(function (d) {
        return id === d.id;
      }).style('fill', function (d) {
        d.currentMetric = statTableCell;
        d.currentMetricValue = value;
        d.tooltipDetails = tooltipDetails;
        return d.color = !_.isNumber(value) || isNaN(value) ? NO_DATA_COLOR : statTableCell.metric.colorScale(value);
      });
    }

    function colorTopLevel(value, statTableCell) {
      topLevelStatus.filter(function (d) {
        return d === statTableCell;
      }).style('fill', function (d) {
        return d.color = !_.isNumber(value) || isNaN(value) ? NO_DATA_COLOR : statTableCell.metric.colorScale(value);
      }).style('visibility', null);
    }

    function resetColors() {
      d3.select('html').classed('no-colors', true);
      d3.select('#color-key').text('');
      d3.selectAll('#physical-plan .instance, #logical-plan .node').style('fill', function (d) {
        d.currentMetric = null;
        d.currentMetricValue = null;
        return d.color = d.defaultColor;
      });
    }

    function createMetricsUrl(metric, component, instance, start, end) {
      var url = baseUrl + '/topologies/metrics/timeline?';
      return [
        url + 'cluster=' + cluster,
        'environ=' + environ,
        'topology=' + toponame,
        'metric=' + metric.metricName,
        'component=' + component,
        'instance=' + instance,
        'starttime=' + start,
        'endtime=' + end
      ].join('&');
    }

    var outstandingRequests = {};

    // slight optimization, instead of requesting metrics for 3m, 10m, 60m, 1h, 3h
    // always request 3 hours of data on first call and attach all simultaneous
    // stats calls to result from the first one and when that responds, parse out
    // the values over last N minutes.

    function makeMetricsQuery(metric, lookbackMinutes, callback) {
      var start = moment().startOf('minute').subtract(lookbackMinutes, 'minutes').valueOf() / 1000;
      doMake3hourMetricsQuery(metric, function (results) {
        var finalResult = results.map(function (d) {
          var values = d.value.filter(function (datapoint) {
            return datapoint.time >= start;
          }).map(function (d) { return d.value; });
          return {
            name: d.name,
            id: d.id,
            value: p90(values)
          };
        });
        callback(finalResult);
      });
    }

    function doMake3hourMetricsQuery(metric, callback) {
      var queryName = metric.name;
      if (outstandingRequests[queryName]) {
        outstandingRequests[queryName].push(callback);
        return;
      }
      var callbacks = [callback];
      outstandingRequests[queryName] = callbacks;
      var start = moment().startOf('minute').subtract(3, 'hours').valueOf() / 1000;
      var end = moment().startOf('minute').valueOf() / 1000;

      fetchMetrics();

      function fetchMetrics() {
        var url = createMetricsUrl(metric, "*", "*", start, end);
        d3.json(url, function (error, data) {
          if (error) {
            console.warn(error);
          }
          var result = [];
          if (data && data.hasOwnProperty("status") && data["status"] === "success" && data.hasOwnProperty("result")) {
            data.result.timeline.forEach(function (timeline) {
              var split = timeline.instance.split('_');
              var values = [];
              for (var timestamp in timeline.data) {
                if (timeline.data.hasOwnProperty(timestamp)) {
                  values.push({
                    time: timestamp,
                    value: timeline.data[timestamp]
                  });
                }
              }
              result.push({
                id: timeline.instance,
                name: split.slice(2, split.length - 1).join('_'),
                value: values
              });
            })
            resolve(result);
          }
        });
      }

      function resolve(data) {
        delete outstandingRequests[queryName];
        callbacks.forEach(function (cb) { cb(data); });
      }
    }
  }

  global.drawStatsTable = drawStatsTable;
}(this));
