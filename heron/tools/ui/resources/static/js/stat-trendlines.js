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
 * Render trendline for stats below the containers and instances view
 */
function StatTrendlines(baseUrl, cluster, environ, toponame, physicalPlan, logicalPlan) {
  var result = {};
  var target = d3.select('#stat-trendlines').style('text-align', 'center');

  // When a user clicks an instance, render area charts below the containers and instances graphic
  result.showInstance = function (name, instance) {
    var margin = {
      top: 15,
      right: 60,
      bottom: 20,
      left: 80
    };
    var rowHeight = 30;
    var endTime = moment().startOf('minute');
    var startTime = moment().startOf('minute').subtract(60, 'minutes');
    var height = rowHeight * window.pollingMetrics.length;
    var outerWidth = 500, outerHeight = height + margin.top + margin.bottom;
    var width = outerWidth - margin.left - margin.right;
    var outerSvg = target.text('').append('svg')
        .attr('class', 'text-center')
        .attr('width', outerWidth)
        .attr('height', outerHeight)
      .append('g')
        .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');

    var svg = outerSvg.append('g');
    var svgTop = outerSvg.append('g');
    var tip = d3.tip()
        .attr('class', 'd3-tip instance text-center')
        .offset([-8, 0])
        .html(function(d) {
          return moment(d.data[0]).format('h:mma') + ': <strong>' + d.metric.format(d.data[1]) + '</strong> ' + d.metric.legendDescription;
        });
    svg.call(tip);
    svg.append('rect')
        .attr('fill', 'white')
        .attr('stroke', 'white')
        .attr('x', 0)
        .attr('y', 0)
        .attr('width', width)
        .attr('height', height);

    var scale = d3.time.scale()
        .domain([startTime.valueOf(), endTime.valueOf()])
        .range([0, width]);

    var axis = d3.svg.axis()
        .scale(scale)
        .orient('bottom')
        .ticks(d3.time.minute, 15)
        .tickFormat(function (d) {
          return moment(d).format('h:mma');
        })
        .tickSize(-height);

    var rows = svg
        .selectAll('.row')
        .data(window.pollingMetrics)
        .enter()
      .append('g')
        .attr('class', 'row')
        .attr('transform', function (d, i) { return 'translate(0,' + i * rowHeight + ')'; });

    svg.append('g').attr('class', 'x axis').attr('transform', 'translate(0,' + height + ')').call(axis);
    svg.append('defs')
      .append('clipPath')
        .attr('id', 'row-clip')
      .append('rect')
        .attr('x', 0)
        .attr('y', 3)
        .attr('width', width)
        .attr('height', rowHeight - 3);

    rows.append('text')
        .attr('y', (rowHeight / 2) + 3)
        .attr('x', -5)
        .attr('text-anchor', 'end')
        .text(function (d) {
          var name = instance === '*' ? 'Max ' + d.name : d.name;
          return extractWrappedWord(name, true);
        })
        .append('tspan')
        .attr('y', (rowHeight / 2) + 14)
        .attr('x', -5)
        .attr('text-anchor', 'end')
        .text(function (d) {
          var name = instance === '*' ? 'Max ' + d.name : d.name;
          return extractWrappedWord(name, false);
        });
    rows.append('rect')
        .style('fill', 'none')
        .attr('width', width)
        .attr('height', rowHeight);

    var metricValues = {};

    // Request metrics for each row and render it
    rows.each(function (metric, i) {
      // make color gradient
      var extent = d3.extent(metric.scaleTicks);

      var outerRow = d3.select(this);
      var row = outerRow.append('g')
        .attr('clip-path', 'url(#row-clip)');
      outerRow.append('g')
          .attr('class', 'x axis')
        .append('line')
          .attr('x1', 0)
          .attr('x2', width)
          .attr('y1', rowHeight)
          .attr('y2', rowHeight);
      var yScale = d3.scale.linear()
        .domain(extent)
        // yScale starts from 1 so that value 0 is visible and users can tell
        // if metrics are loaded or not
        .range([rowHeight - 1, 0]);

      // request data and render the line chart
      (metric.queryTrendline || makeTrendlineQuery)(metric, name, instance, startTime, endTime, function (data) {
        metricValues[metric.name] = data;

        var rectScale = d3.scale.ordinal()
            .domain(data.map(function (d) { return d[0].getTime(); }))
            .rangeRoundBands([0, width], 0.1, 0);

        var bars = row.selectAll('.data-bar')
            .data(data)
            .enter()
          .append('g')
            .attr('class', 'data-bar')
            .attr('transform', function (d) {
              return 'translate(' + rectScale(d[0].getTime()) + ',0)';
            });
        var clipY = d3.scale.linear()
            .domain([0, rowHeight * 2 / 3])
            .range([0, rowHeight * 2 / 3])
            .clamp(true);
        var clipHeight = d3.scale.linear()
            .domain([0, rowHeight / 3])
            .range([0, rowHeight / 3])
            .clamp(true);
        bars.append('rect')
            .each(function (d) { d.target = this; })
            .attr('x', 0)
            .attr('y', function (d) { return clipY(yScale(d[1])); })
            .attr('height', function (d) { return clipHeight(height - yScale(d[1])); })
            .style('fill', 'white')
            .style('stroke', 'none')
            .attr('width', rectScale.rangeBand());

        bars.append('rect')
            .attr('x', 0)
            .attr('width', rectScale.rangeBand())
            .attr('y', function (d) { return yScale(d[1]); })
            .attr('height', function (d) { return height - yScale(d[1]); })
            .style('stroke', 'none')
            .style('opacity', 1)
            .style('fill', function (d) { return metric.colorScale(d[1]); });
        var timeout;
        bars.on('mouseover', function (d) {
          tip.show({data: d, metric: metric}, d.target);
          rows.selectAll('rect').style('opacity', function (other) {
            return other === d ? 1 : 0.3;
          });
          clearTimeout(timeout);
        }).on('mouseout', function () {
          clearTimeout(timeout);
          timeout = setTimeout(function () {
            tip.hide();
            rows.selectAll('rect').style('opacity', 1);
          }, 100);
        });
      });
    });

    var valueIndicators = rows.append('text')
      .attr('y', rowHeight / 2 + 5)
      .attr('dx', 5)
      .style('opacity', 0);

    // hide the reset button when the entire topology view selected
    d3.select('html').classed('all-topo-view', name === '*');

    if (name === '*') {
      d3.select('#trendline-title').text('Topology Metrics');
    } else if (instance === '*') {
      d3.select('#trendline-title').text(name + ' Metrics');
    } else {
      d3.select('#trendline-title').text(instance + ' Metrics');

      container = instance.split("_")[1];
      target
        .append('div')
        .attr('class', 'text-center')
        .html([
          '<a class="btn btn-primary btn-xs" target="_blank" href="' + baseUrl + '/topologies/' + cluster + '/' + environ + '/' + toponame + '/' + container + '/file?path=./log-files/' + instance + '.log.0">logs</a>',
          '<a class="btn btn-primary btn-xs" target="_blank" href="' + baseUrl + '/topologies/filestats/' + cluster + '/' + environ + '/' + toponame + '/' + container + '/file">files</a>',
          '<a class="btn btn-primary btn-xs" target="_blank" href="' + baseUrl + '/topologies/' + cluster + '/' + environ + '/' + toponame + '/' + name + '/' + instance + '/exceptions">exceptions</a>',
          '<br>',
        ].join(' '));
    }
  };

  // In case input name contains more than 2 words, then
  // only first 2 words are printed in one line and all
  // the other words in next line. Used to solve text-wrap issue.
  function extractWrappedWord(name, isFirstLine) {
    if (name == null || name.trim().length == 0){
      return name;
    }
    var words = name.split(' ');
    if(words.length >= 3 && isFirstLine){
      return words[0] + ' ' + words[1];
    }
    if(words.length >= 3){
      var remainingWords = words.slice(2, words.length);
      return remainingWords.join(' ');
    }
    if(isFirstLine){
      return name;
    }
    return '';
  }

  function makeTrendlineQuery(metric, name, instance, start, end, callback) {
    var minuteToIndex = {};
    start = start.valueOf() / 1000;
    end = end.valueOf() / 1000;
    for (var i = start, j = 0; i < end; i+=60, j++) {
      minuteToIndex[i] = j;
    }

    function round(n) {
      return Math.floor(n / 60) * 60;
    }

    executeMetricsQuery();

    function executeMetricsQuery() {
      var u = baseUrl + '/topologies/metrics/timeline?'
      var request = [
        u + 'cluster=' + cluster,
        'environ=' + environ,
        'topology=' + toponame,
        'metric=' + metric.metricName,
        'component=' + name,
        'instance=' + instance,
        'starttime=' + start,
        'endtime=' + end,
        'max=' + true
      ].join('&');
      d3.json(request, function (error, data) {
        var result = [];
        if (data && data.hasOwnProperty("status") && data["status"] === "success" && data.hasOwnProperty("result")) {
          var result = [];
          d3.entries(minuteToIndex).forEach(function (d) {
            result[d.value] = [new Date((+d.key) * 1000), 0];
          });
          var timeline = data.result.timeline[0];
          for (var timestamp in timeline.data) {
            if (timeline.data.hasOwnProperty(timestamp)) {
              if (result[minuteToIndex[round(timestamp)]]) {
                result[minuteToIndex[round(timestamp)]][1] = timeline.data[timestamp];
              }
            }
          }
          resolve(result);
        }
      });
    }

    function resolve(data) {
      callback(data);
      callback = function () {};
    }
  }
  return result;
}
