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
 * Code for drawing the enclosed rectangles that make up the physical plan for this topology.
 * Exports a single global function drawPhysicalPlan.
 */
(function (global) {
  var topo, instances, containers;

  function drawPhysicalPlan(planController, data, packingData, div_id, outerWidth, minHeight, cluster, environ, toponame) {
    var margin = {
      top: 50,
      right: 0,
      bottom: 20,
      left: 0
    };
    var width = outerWidth - margin.left - margin.right;

    instances = data.result.instances;
    containers = data.result.stmgrs;
    containerPlan = packingData.container_plans;

    var sptblt = Object.keys(data.result.spouts)
                       .concat(Object.keys(data.result.bolts));

    var color = d3.scale.ordinal().domain(sptblt.sort()).range(colors);

    for (var ck in containers) {
      var container = containers[ck];
      container.children = [];
    }

    for (var i in sptblt) {
      var comp = sptblt[i];
      for (var wk in instances) {
        var worker = instances[wk];
        if (worker.name === comp) {
          containers[worker.stmgrId].children.push(worker);
        }
      }
    }

    var containerList = _.values(containers);
    containerList.forEach(function (container) {
      // make ordering of instances the same in each container
      container.children = _.sortBy(container.children, 'name');
      // Parse index
      container.index = parseInt(container.id.split('-')[1]);
      // Search for container and instance resource config in packing plan
      container.required_resources = {
        'cpu': 'unknown',
        'disk': 'unknown',
        'ram': 'unknown'
      };
      for (var i in containerPlan) {
        var packing = containerPlan[i];
        if (packing.id === container.index) {
          container.required_resources = packing.required_resources;
          // Get instance resources
          var instance_resources = {};
          for (var j in packing.instances) {
            var inst = packing.instances[j];
            instance_resources[inst.component_name + '_' + inst.task_id] = inst.instance_resources;
          }
          container.instance_resources = instance_resources;
        }
      }
    });

    // Sort the containers by their id so they are easier to find in the UI.
    containerList = _.sortBy(containerList, function(container) {
      return container.index;
    });

    var maxInstances = d3.max(containerList, function (d) {
      return d.children.length;
    });

    function formatByteSize(byteSize) {
      if (byteSize > 1024 * 1024 * 1024 / 2) {
        return (byteSize / (1024 * 1024 * 1024)).toFixed(2) + 'GB';
      } else if (byteSize > 1024 * 1024 / 2) {
        return (byteSize / (1024 * 1024)).toFixed(2) + 'MB';
      } else if (byteSize > 1024 / 2) {
        return (byteSize / 1024).toFixed(2) + 'KB';
      } else {
        return byteSize + 'B';
      }
    }

    /**
     * Config paramaters for container/heron instance layout
     */

    // margin outside of each container as a fraction of the container width
    var containerMarginRatio = 0.08;
    // padding inside each container as a fraction of container width
    var containerPaddingRatio = 0.05;
    // margin around each heron instance as a faction of the instance width
    var instanceMarginRatio = 0.15;
    // smallest allowable width for a heron instance (below, this and we expand vertically)
    var minInstanceWidth = 6;
    // largest allowable width for heron instance (above this we shrink)
    var maxInstanceWith = 30;

    /**
     * Compute the container and instance sizes
     */

    var innerCols = Math.ceil(Math.sqrt(maxInstances));
    // compute the min columns allowed to keep instance size below limit
    var minContainerCols = Math.ceil(width / (innerCols * maxInstanceWith / (1 - 2 * instanceMarginRatio)));
    // compute the max columns allowed to keep instance size above min limit
    var maxContainerCols = Math.ceil(width / (innerCols * minInstanceWidth / (1 - 2 * instanceMarginRatio)));
    // compute # cols to give 50% more columns than rows, and bound it by max/min columns
    var desiredContainerCols = Math.ceil(Math.sqrt(containerList.length) * 1.5);
    var cols = Math.min(maxContainerCols, Math.max(minContainerCols, desiredContainerCols));
    // from there, compute the required number of rows and exact instance/container sizes...
    var rows = Math.ceil(containerList.length / cols);
    var innerRows = Math.ceil(maxInstances / innerCols);
    var containerWidth = (width / cols) * (1 - 2 * containerMarginRatio);
    var containerPadding = containerWidth * containerPaddingRatio;
    var containerMargin = containerMarginRatio * containerWidth;
    var instanceSize = (containerWidth - containerPadding * 2) / innerCols;
    var containerHeight = instanceSize * innerRows + containerPadding * 2;
    var height = (containerHeight + containerMargin * 2) * rows;
    var totalVisHeight = height + margin.top + margin.bottom;
    var totalVisWidth = Math.min(width, containerList.length * (containerWidth + containerMargin * 2));

    var svg = d3.select(div_id).text("").append("svg")
        .attr("width", totalVisWidth)
        .attr("height", totalVisHeight)
        .style("z-index", "1")
      .append("g")
        .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');

    var textContainer = svg.append('text')
      .attr('x', (totalVisWidth - margin.left - margin.right) / 2)
      .attr('y', height + 25)
      .style('text-anchor', 'middle');

    var instance_tip = d3.tip()
        .attr('class', 'd3-tip main text-center')
        .offset([8, 0])
        .direction('s')
        .html(function(d) {
          var rows = window.pollingMetrics;
          var cols = window.pollingPeriods;
          var result = d.id + '<br>';
          if (d.currentMetric && _.isNumber(d.currentMetricValue)) {
            result += '<strong>' + d.currentMetric.metric.format(d.currentMetricValue) + '</strong>';
            result += ' ';
            result += d.currentMetric.metric.legendDescription;
            result += ' over last ';
            result += d.currentMetric.time.name;
            if (d.tooltipDetails) {
              result += d.tooltipDetails;
            }
          }
          return result;
        });

    var container_tip = d3.tip()
        .attr('class', 'd3-tip main container')
        .offset([8, 0])
        .direction('s')
        .html(function (container) {
          var instance_cpu = '';
          var instance_ram = '';
          var instance_disk = '';
          if (container.instance_resources) {
            for (var inst in container.instance_resources) {
              instance_cpu += '<li>   - ' + inst + ': <span>' + container.instance_resources[inst].cpu + '</span></li>';
              instance_ram += '<li>   - ' + inst + ': <span>' + formatByteSize(container.instance_resources[inst].ram) + '</span></li>';
              instance_disk += '<li>   - ' + inst + ': <span>' + formatByteSize(container.instance_resources[inst].disk) + '</span></li>';
            }
          }
          return '<ul>' +
              '<li>cpu required: <span>' + container.required_resources.cpu + '</span></li>' + instance_cpu +
              '<li>ram required: <span>' + formatByteSize(container.required_resources.ram) + '</span></li>' + instance_ram +
              '<li>disk required: <span>' + formatByteSize(container.required_resources.disk) + '</span></li>' + instance_disk +
              '</ul>';
        });

    var containerGs = svg.selectAll('.physical-plan')
        .data(containerList, function (d) { return d.id; })
        .enter()
      .append('g')
        .attr('class', 'physical-plan')
        .attr('transform', function (d, i) {
          var x = (i % cols) * (containerWidth + 2 * containerMargin) + containerMargin;
          var y = Math.floor(i / cols) * (containerHeight + 2 * containerMargin) + containerMargin;
          return 'translate(' + x + ',' + y + ')';
        });

    containerGs.append('rect')
        .attr('width', containerWidth)
        .attr('height', containerHeight)
        .attr('class', 'container')
        .on('mouseover', container_tip.show)
        .on('mouseout', container_tip.hide);

    containerGs.selectAll('.instance')
        .data(function (d) { return d.children; }, function (d) { return d.id; })
        .enter()
      .append('rect')
        .attr('class', 'instance')
        .style('fill', function (d) {
          d.defaultColor = color(d.name);
          d.color = d.color || d.defaultColor;
          return d.color;
        })
        .attr('width', instanceSize * (1 - 2 * instanceMarginRatio))
        .attr('height', instanceSize * (1 - 2 * instanceMarginRatio))
        .attr('x', function (d, i) { return containerPadding + (i % innerCols) * instanceSize + instanceSize * instanceMarginRatio; })
        .attr('y', function (d, i) { return containerPadding +Math.floor(i / innerCols) * instanceSize + instanceSize * instanceMarginRatio; })
        .on('mouseover', function (d) {
          planController.physicalComponentHoverOver(d, instance_tip);
        })
        .on('mouseout', function (d) {
          planController.physicalComponentHoverOut(d, instance_tip);
        })
        .on('click', function (d) {
          planController.physicalComponentClicked(d);
        });

    svg.call(instance_tip);
    svg.call(container_tip);
  }

  // Stash the old values for transition.
  function stash(d) {
    d.x0 = d.x;
    d.dx0 = d.dx;
  }

  global.drawPhysicalPlan = drawPhysicalPlan;
}(this));
