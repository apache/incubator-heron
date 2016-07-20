/**
 * Code for drawing the enclosed rectangles that make up the physical plan for this topology.
 * Exports a single global function drawPhysicalPlan.
 */
(function (global) {
  var topo, instances, containers;

  function drawPhysicalPlan(planController, data, div_id, outerWidth, minHeight, cluster, environ, toponame) {
    var margin = {
      top: 50,
      right: 0,
      bottom: 20,
      left: 0
    };
    var width = outerWidth - margin.left - margin.right;

    instances = data.result.instances;
    containers = data.result.stmgrs;

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

    var auroraContainers = _.values(containers);
    auroraContainers.forEach(function (container) {
      // make ordering of instances the same in each container
      container.children = _.sortBy(container.children, 'name');
    });

    // Sort the containers by their id so they are easier to find in the UI.
    auroraContainers = _.sortBy(auroraContainers, function(container) {
      return parseInt(container.id.split('-')[1]);
    });

    var maxInstances = d3.max(auroraContainers, function (d) {
      return d.children.length;
    });

    /**
     * Config paramaters for aurora container/heron instance layout
     */

    // margin outside of each aurora container as a fraction of the container width
    var containerMarginRatio = 0.08;
    // padding inside each aurora container as a fraction of container width
    var containerPaddingRatio = 0.05;
    // margin around each heron instance as a faction of the instance width
    var instanceMarginRatio = 0.15;
    // smallest allowable width for a heron instance (below, this and we expand vertically)
    var minInstanceWidth = 6;
    // largest allowable width for heron instance (above this we shrink)
    var maxInstanceWith = 30;

    /**
     * Compute the aurora container and instance sizes
     */

    var innerCols = Math.ceil(Math.sqrt(maxInstances));
    // compute the min aurora columns allowed to keep instance size below limit
    var minAuroraCols = Math.ceil(width / (innerCols * maxInstanceWith / (1 - 2 * instanceMarginRatio)));
    // compute the max aurora columns allowed to keep instance size above min limit
    var maxAuroraCols = Math.ceil(width / (innerCols * minInstanceWidth / (1 - 2 * instanceMarginRatio)));
    // compute # cols to give 50% more columns than rows, and bound it by max/min aurora columns
    var desiredAuroraCols = Math.ceil(Math.sqrt(auroraContainers.length) * 1.5);
    var cols = Math.min(maxAuroraCols, Math.max(minAuroraCols, desiredAuroraCols));
    // from there, compute the required number of rows and exact instance/container sizes...
    var rows = Math.ceil(auroraContainers.length / cols);
    var innerRows = Math.ceil(maxInstances / innerCols);
    var containerWidth = (width / cols) * (1 - 2 * containerMarginRatio);
    var containerPadding = containerWidth * containerPaddingRatio;
    var containerMargin = containerMarginRatio * containerWidth;
    var instanceSize = (containerWidth - containerPadding * 2) / innerCols;
    var containerHeight = instanceSize * innerRows + containerPadding * 2;
    var height = (containerHeight + containerMargin * 2) * rows;
    var totalVisHeight = height + margin.top + margin.bottom;
    var totalVisWidth = Math.min(width, auroraContainers.length * (containerWidth + containerMargin * 2));

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

    var tip = d3.tip()
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

    var auroraGs = svg.selectAll('.aurora-container')
        .data(auroraContainers, function (d) { return d.id; })
        .enter()
      .append('g')
        .attr('class', 'aurora-container')
        .attr('transform', function (d, i) {
          var x = (i % cols) * (containerWidth + 2 * containerMargin) + containerMargin;
          var y = Math.floor(i / cols) * (containerHeight + 2 * containerMargin) + containerMargin;
          return 'translate(' + x + ',' + y + ')';
        });

    auroraGs.append('rect')
        .attr('width', containerWidth)
        .attr('height', containerHeight)
        .attr('class', 'aurora');


    auroraGs.selectAll('.instance')
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
          planController.physicalComponentHoverOver(d, tip);
        })
        .on('mouseout', function (d) {
          planController.physicalComponentHoverOut(d, tip);
        })
        .on('click', function (d) {
          planController.physicalComponentClicked(d);
        });



    // put tooltip on top of everything
    d3.selectAll('.d3-tip.main').remove();
    svg.call(tip);
  }

  // Stash the old values for transition.
  function stash(d) {
    d.x0 = d.x;
    d.dx0 = d.dx;
  }

  global.drawPhysicalPlan = drawPhysicalPlan;
}(this));
