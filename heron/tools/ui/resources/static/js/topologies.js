/** @jsx React.DOM */

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

var SUM = 0,
    AVG = 1,
    LAST = 2;

var countersUrlFlags = {};
setInterval(function() {
  // Reset fetch flags every minute so that new fetches can go through
  countersUrlFlags = {};
}, 60000);

const ParallelismConfig = "topology.component.parallelism";

var AllExceptions = React.createClass({
  getInitialState: function() {
    return {}
  },
  fetchExceptionSummary: function() {
    var compName = this.props.info.comp_name ? this.props.info.comp_name : 'All';
    var fetchUrl = './' + this.props.info.topology
        + '/' + compName + '/exceptionsummary.json'
    console.log('fetching url ' + fetchUrl);
    $.ajax({
        url: fetchUrl,
        dataType:  'json',

        success: function(response) {
          this.state.summary = response.result
          console.log('RESPONSE')
          console.log(response)
        }.bind(this),

        error: function() {
        }.bind(this)
      });
  },
  componentWillMount:function() {
    this.fetchExceptionSummary();
  },
  componentDidUpdate: function(prevProps, prevState) {
    if (prevProps.info.comp_name != this.props.info.comp_name) {
      this.fetchExceptionSummary();
    }
  },
  render: function() {
    if (!this.state.summary) {
      return <div></div>
    }
    var compName = this.props.info.comp_name ? this.props.info.comp_name : 'All';
    var exceptionTable = this.state.summary;
    var allExceptionsUrl = './' + this.props.info.topology
        + '/' + compName + '/All/exceptions';
    var allExceptionsStyle = {
      color: 'black',
      background: 'lightblue',
      padding: '5px',
      display: 'inline',
      boxShadow: '1px 2px 2px',
    }
    return (
      <div>
        <div className="widget-header">
          <div className="title">
            <h4>Recent Exception Counts</h4>
          </div>
        </div>
        <table className="table table-striped table-hover no-margin">
          <thead>
            <tr>
              <th>
                Exceptions Summary {
                  (self.aggregate && self.aggregate.length > 0)
                  ? <a href={allExceptionsUrl}> <div style={allExceptionsStyle}>Expand</div> </a>
                  : null
                }
              </th>
              <th>Total Count</th>
            </tr>
          </thead>
          <tbody>
            {exceptionTable.map(
              function (row) {
                return <tr>
                {
                  row.map(function (value) {
                    return <td className="col-md-2">{value}</td>;
                  })
                }</tr>;
              })
            }
          </tbody>
        </table>
      </div>
    );
  },
});

var AllMetrics = React.createClass({
  getInitialState: function () {

    this.supportedMetricNames = {
      "__emit-count/": {
        name: "Emit Count",
        scaleDevisor: 1,
        aggregationType: SUM
      },
      "__complete-latency/": {
        name: "Complete Latency (ms)",
        scaleDevisor: 1000000,
        aggregationType: AVG
      },
      "__ack-count/": {
        name: "Ack Count",
        scaleDevisor: 1,
        aggregationType: SUM
      },
      "__execute-count/": {
        name: "Execute Count",
        scaleDevisor: 1,
        aggregationType: SUM
      },
      "__fail-count/": {
        name: "Fail Count",
        scaleDevisor: 1,
        aggregationType: SUM
      },
      "__execute-latency/": {
        name: "Execute Latency (ms)",
        scaleDevisor: 1000000,
        aggregationType: AVG
      },
      "__process-latency/": {
        name: "Process Latency (ms)",
        scaleDevisor: 1000000,
        aggregationType: AVG
      },
      "__jvm-uptime-secs": {
        name: "Uptime (ddd:hh:mm:ss)",
        scaleDevisor: 1,
        aggregationType: LAST
      },
    };

    this.spoutMetrics = {
      "__emit-count/": {
        name: "Emit Count",
        scaleDevisor: 1,
        aggregationType: SUM
      },
      "__complete-latency/": {
        name: "Complete Latency (ms)",
        scaleDevisor: 1000000,
        aggregationType: AVG
      },
      "__ack-count/": {
        name: "Ack Count",
        scaleDevisor: 1,
        aggregationType: SUM
      }
    };

    this.boltMetricsOutput = {
      "__emit-count/": {
        name: "Emit Count",
        scaleDevisor: 1,
        aggregationType: SUM
      }
    };

    this.boltMetricsInput = {
      "__execute-count/": {
        name: "Execute Count",
        scaleDevisor: 1,
        aggregationType: SUM
      },
      "__ack-count/": {
        name: "Ack Count",
        scaleDevisor: 1,
        aggregationType: SUM
      },
      "__fail-count/": {
        name: "Fail Count",
        scaleDevisor: 1,
        aggregationType: SUM
      },
      "__execute-latency/": {
        name: "Execute Latency (ms)",
        scaleDevisor: 1000000,
        aggregationType: AVG
      },
      "__process-latency/": {
        name: "Process Latency (ms)",
        scaleDevisor: 1000000,
        aggregationType: AVG
      },
    };

    this.tenMins = "10 mins";
    this.threeHrs = "3 hrs";
    this.oneHr = "1 hr";
    this.allTime = "All Time";

    this.timeRanges = {}
    this.timeRanges[this.tenMins] = 10 * 60;
    this.timeRanges[this.oneHr] = 1 * 60 * 60;
    this.timeRanges[this.threeHrs] = 3 * 60 * 60;
    this.timeRanges[this.allTime] = -1;

    return {
        metrics: {},
        lplan: undefined,
        pplan: undefined
    };
  },

  /**
   * Debounce rendering the table (expensive for large topologies) every time a metrics
   * response is received.
   */
  metrics: {},
  renderMetricsTimeout: null,
  setMetrics: function (metrics) {
    this.metrics = metrics;
    clearTimeout(this.renderMetricsTimeout);
    this.renderMetricsTimeout = setTimeout(function () {
      // perform the expensive operation
      this.setState({metrics: this.metrics});
    }.bind(this), 1000);
  },

  componentWillMount: function () {
    this.fetchLplan();
    this.fetchPplan();
  },

  componentDidUpdate: function(prevProps, prevState) {
    if (prevProps.topology != this.props.topology) {
      this.fetchLplan();
      return;
    }

    if (prevProps.comp_name != this.props.comp_name) {
      this.fetchLplan();
      return;
    }

    if (prevProps.comp_instance != this.props.comp_instance) {
      this.fetchLplan();
    }
  },

  fetchLplan: function () {
    if (this.state.lplan === undefined) {
      const fetch_url = this.props.baseUrl +
       "/topologies/" +
        this.props.cluster + "/" +
        this.props.environ + "/" +
        this.props.topology + "/" +
        "logicalplan.json";
      $.ajax({
        url:       fetch_url,
        dataType:  'json',
        success: function(response) {
          this.state.lplan = response.result;
          var components = [];
          for (var spout in this.state.lplan.spouts) {
            if (this.state.lplan.spouts.hasOwnProperty(spout)) {
              components.push({
                comp: spout,
                type: "spout"
              });
            }
          }
          for (var bolt in this.state.lplan.bolts) {
            if (this.state.lplan.bolts.hasOwnProperty(bolt)) {
              components.push({
                comp: bolt,
                type: "bolt"
              });
            }
          }
          var metrics = this.metrics;
          for (var i = 0; i < components.length; i++) {
            var component = components[i].comp;
            var type = components[i].type;
            if (!metrics.hasOwnProperty(component)) {
              metrics[component] = {};
            }
            for (var timeRange in this.timeRanges) {
              if (this.timeRanges.hasOwnProperty(timeRange)) {
                if (!metrics[component].hasOwnProperty(timeRange)) {
                  metrics[component][timeRange] = {};
                }
                if (type === "spout") {
                  for (var metricname in this.spoutMetrics) {
                    if (this.spoutMetrics.hasOwnProperty(metricname)) {
                      var displayName = this.spoutMetrics[metricname].name;
                      if (!metrics[component][timeRange].hasOwnProperty(displayName)) {
                        metrics[component][timeRange][displayName] = {};
                      }
                      for (var j = 0; j < this.state.lplan.spouts[component].outputs.length; j++) {
                        var streamName = this.state.lplan.spouts[component].outputs[j].stream_name;
                        if (!metrics[component][timeRange][displayName].hasOwnProperty(streamName)) {
                          metrics[component][timeRange][displayName][streamName] = {};
                        }
                      }
                    }
                  }
                } else {
                  for (var metricname in this.boltMetricsOutput) {
                    if (this.boltMetricsOutput.hasOwnProperty(metricname)) {
                      var displayName = this.boltMetricsOutput[metricname].name;
                      if (!metrics[component][timeRange].hasOwnProperty(displayName)) {
                        metrics[component][timeRange][displayName] = {};
                      }
                      for (var j = 0; j < this.state.lplan.bolts[component].outputs.length; j++) {
                        var streamName = this.state.lplan.bolts[component].outputs[j].stream_name;
                        if (!metrics[component][timeRange][displayName].hasOwnProperty(streamName)) {
                          metrics[component][timeRange][displayName][streamName] = {};
                        }
                      }
                    }
                  }
                  for (var metricname in this.boltMetricsInput) {
                    if (this.boltMetricsInput.hasOwnProperty(metricname)) {
                      var displayName = this.boltMetricsInput[metricname].name;
                      if (!metrics[component][timeRange].hasOwnProperty(displayName)) {
                        metrics[component][timeRange][displayName] = {};
                      }
                      for (var j = 0; j < this.state.lplan.bolts[component].inputs.length; j++) {
                        var streamName = this.state.lplan.bolts[component].inputs[j].stream_name;
                        if (!metrics[component][timeRange][displayName].hasOwnProperty(streamName)) {
                          metrics[component][timeRange][displayName][streamName] = {};
                        }
                      }
                    }
                  }
                }
              }
            }
          }
          this.setState({metrics: metrics});
          this.fetchCounters();
        }.bind(this),

        error: function() {
        }
      });
    } else {
      this.fetchCounters();
    }
  },

  fetchPplan: function () {
    url = this.props.baseUrl +
      "/topologies/" +
      this.props.cluster + "/" +
      this.props.environ + "/" +
      this.props.topology + "/" +
      "physicalplan.json";
    $.ajax({
      url: url,
      dataType: 'json',
      success: function (response) {
        var pplan = response.result;
        this.setState({pplan: pplan});
        this.fetchCounters();
      }.bind(this),

      error: function () {
      }
    });
  },

  fetchCounters: function() {
    if (this.props.hasOwnProperty("comp_name")) {
      const fetch_url = this.props.baseUrl + "/topologies/metrics?" +
        "cluster=" + this.props.cluster + "&" +
        "environ=" + this.props.environ + "&" +
        "topology=" + this.props.topology + "&" +
        "component=" + this.props.comp_name;
      var metricnames = [];

      // Explicitly add JVM uptime metric since it
      // is unique, in the sense that no stream is
      // associated with it.
      metricnames.push("__jvm-uptime-secs");
      if (this.props.comp_type === "spout") {
        var spout = this.props.comp_name;
        for (var i = 0; i < this.state.lplan.spouts[spout].outputs.length; i++) {
          var streamName = this.state.lplan.spouts[spout].outputs[i].stream_name;
          for (var spoutMetric in this.spoutMetrics) {
            if (this.spoutMetrics.hasOwnProperty(spoutMetric)) {
              var metricname = spoutMetric + streamName;
              metricnames.push(metricname);
            }
          }
        }
      } else {
        var bolt = this.props.comp_name;
        for (var i = 0; i < this.state.lplan.bolts[bolt].outputs.length; i++) {
          var streamName = this.state.lplan.bolts[bolt].outputs[i].stream_name;
          for (var boltMetric in this.boltMetricsOutput) {
            if (this.boltMetricsOutput.hasOwnProperty(boltMetric)) {
              var metricname = boltMetric + streamName;
              metricnames.push(metricname);
            }
          }
        }
        for (var i = 0; i < this.state.lplan.bolts[bolt].inputs.length; i++) {
          var streamName = this.state.lplan.bolts[bolt].inputs[i].stream_name;
          for (var boltMetric in this.boltMetricsInput) {
            if (this.boltMetricsInput.hasOwnProperty(boltMetric)) {
              var metricname = boltMetric + streamName;
              metricnames.push(metricname);
            }
          }
        }
      }
      for (var timeRange in this.timeRanges) {
        metricnameargs = "";
        for (var i = 0; i < metricnames.length; i++) {
          metricnameargs += "&metricname=" + metricnames[i];
        }
        if (this.timeRanges.hasOwnProperty(timeRange) && metricnameargs != "") {
          var url = fetch_url + metricnameargs +
            "&interval=" + this.timeRanges[timeRange];
          this.fetchCountersURL(url, timeRange);
        }
      }
    } else if (!this.props.hasOwnProperty("comp_name")) {
      // Fetch all metrics for all the spouts.
      // Must have pplan and metricnames to continue.
      if (this.state.pplan && this.state.lplan) {
        const fetch_url = this.props.baseUrl + "/topologies/metrics?" +
          "cluster=" + this.props.cluster + "&" +
          "environ=" + this.props.environ + "&" +
          "topology=" + this.props.topology;

        var spoutNames = [];
        for (var name in this.state.lplan.spouts) {
          if (this.state.lplan.spouts.hasOwnProperty(name)) {
            spoutNames.push(name);
          }
        }

        // Load metrics for spouts.
        for (var i = 0; i < spoutNames.length; i++) {
          var spout = spoutNames[i];
          for (var j = 0; j < this.state.lplan.spouts[spout].outputs.length; j++) {
            var streamName = this.state.lplan.spouts[spout].outputs[j].stream_name;
            for (var timeRange in this.timeRanges) {
              metricnameargs = "";
              for (var spoutMetric in this.spoutMetrics) {
                if (this.spoutMetrics.hasOwnProperty(spoutMetric)) {
                  metricnameargs += "&metricname=" + spoutMetric + streamName;
                }
              }
              if (this.timeRanges.hasOwnProperty(timeRange) && metricnameargs != "") {
                var url = fetch_url + metricnameargs +
                  "&component=" + spout +
                  "&interval=" + this.timeRanges[timeRange];

                this.fetchCountersURL(url, timeRange);
              }
            }
          }
        }
        // Load metrics for bolts.
        var boltNames = [];
        for (var name in this.state.lplan.bolts) {
          if (this.state.lplan.bolts.hasOwnProperty(name)) {
            boltNames.push(name);
          }
        }

        for (var i = 0; i < boltNames.length; i++) {
          var bolt = boltNames[i];
          for (var j = 0; j < this.state.lplan.bolts[bolt].outputs.length; j++) {
            var streamName = this.state.lplan.bolts[bolt].outputs[j].stream_name;
            for (var timeRange in this.timeRanges) {
              metricnameargs = "";
              for (var boltMetric in this.boltMetricsInput) {
                if (this.boltMetricsInput.hasOwnProperty(boltMetric)) {
                  metricnameargs += "&metricname=" + boltMetric + streamName;
                }
              }
              if (this.timeRanges.hasOwnProperty(timeRange) && metricnameargs != "") {
                var url = fetch_url + metricnameargs +
                  "&component=" + bolt +
                  "&interval=" + this.timeRanges[timeRange];

                this.fetchCountersURL(url, timeRange);
              }
            }
          }
        }
      }
    }
  },

  fetchCountersURL: function(url, timeRange) {
    // This function might be called multiple times with the same url. Note
    // that the timeRange value is inlcuded in the url so it can be ignored in
    // the check. We check if the request has been made before first and
    // skip if this is a duplicated request.
    if (!countersUrlFlags[url]) {
      countersUrlFlags[url] = true;  // Set the flag

      $.ajax({
        url:       url,
        dataType:  'json',
        success: function(response) {
          if (response.hasOwnProperty("metrics")) {
            var component = response.component;
            var metrics = this.metrics;
            if (!metrics.hasOwnProperty(component)) {
              metrics[response.component] = {};
            }
            if (!metrics[component].hasOwnProperty(timeRange)) {
              metrics[component][timeRange] = {};
            }
            for (var name in response.metrics) {
              if (response.metrics.hasOwnProperty(name)) {
                var metricname = name;
                // Handle __jvm-uptime-secs as a special case.
                if (name !== "__jvm-uptime-secs") {
                  metricname = name.split("/")[0] + "/";
                }
                var displayName = this.supportedMetricNames[metricname].name;
                if (!metrics[component][timeRange].hasOwnProperty(displayName)) {
                  metrics[component][timeRange][displayName] = {};
                }
                var tmpMetrics = {
                  metrics: response.metrics[name],
                  scaleDevisor: this.supportedMetricNames[metricname].scaleDevisor,
                  aggregationType: this.supportedMetricNames[metricname].aggregationType
                };
                if (name === "__jvm-uptime-secs") {
                  metrics[component][timeRange][displayName][""] = tmpMetrics;
                } else {
                  metrics[component][timeRange][displayName][name.split("/")[1]] = tmpMetrics;
                }
              }
            }
            metrics[component][timeRange]["__interval"] = response.interval;
            this.setMetrics(metrics);
          }
        }.bind(this),

        error: function() {
        }
      });
    }
  },

  render: function() {
    if (!this.state.lplan) {
      this.fetchLplan();
      return (<div></div>);
    }
    if (!this.state.pplan) {
      this.fetchPplan();
      return (<div></div>);
    }
    var info = {
      topology: this.props.topology,
      baseUrl: this.props.baseUrl,
      comp_type: this.props.comp_type,
      comp_name: this.props.comp_name,
      comp_spout_type: this.props.comp_spout_type,
      comp_spout_source: this.props.comp_spout_source,
      cluster: this.props.cluster,
      environ: this.props.environ,
      metrics: this.state.metrics,
      lplan: this.state.lplan,
      pplan: this.state.pplan,
      instance: this.props.instance,
    };
    return (
      <div className="display-info display-counters">
        <TopologyCounters info={info} />
        <SpoutRunningInfo info={info} />
        <BoltRunningInfo info={info} />
        <ComponentCounters info={info} />
        <InstanceCounters info={info} />
      </div>
    );
  }
});


var TopologyCounters = React.createClass({
  capitalize: function(astr) {
    if (astr) {
        return astr.charAt(0).toUpperCase() + astr.slice(1);
    }
    return undefined;
  },

  getTitle: function () {
    return "Topology Counters";
  },

  getTopologyMetricsRows: function () {
    const metrics = this.props.info.metrics;
    const timeRanges = ["10 mins", "1 hr", "3 hrs", "All Time"];
    var aggregatedMetrics = {};

    // Get spout names.
    var spoutNames = [];
    if (this.props.info.pplan) {
      for (var name in this.props.info.pplan.spouts) {
        if (this.props.info.pplan.spouts.hasOwnProperty(name)) {
          spoutNames.push(name);
        }
      }
    }

    for (var s = 0; s < spoutNames.length; s++) {
      var spoutName = spoutNames[s];
      for (var time in metrics[spoutName]) {
        if (metrics[spoutName].hasOwnProperty(time)) {
          for (var metricname in metrics[spoutName][time]) {
            if (metrics[spoutName][time].hasOwnProperty(metricname)
                && metricname !== "__interval"
                && metricname !== "Uptime (ddd:hh:mm:ss)") {
              var value = 0;
              var count = 0;
              var displayName = metricname;
              for (var stream in metrics[spoutName][time][metricname]) {
                if (metrics[spoutName][time][metricname].hasOwnProperty(stream)) {
                  var allMetrics = metrics[spoutName][time][metricname][stream].metrics;
                  var aggregationType = metrics[spoutName][time][metricname][stream].aggregationType;
                  for (var m in allMetrics) {
                    if (allMetrics.hasOwnProperty(m)) {
                      value += Number(allMetrics[m]);
                      count++;
                    }
                  }
                  value /= (metrics[spoutName][time][metricname][stream].scaleDevisor || 1);
                }
              }
              if (aggregationType === AVG && count > 0) {
                value /= count;
              }
              if (!aggregatedMetrics.hasOwnProperty(displayName)) {
                aggregatedMetrics[displayName] = {};
              }
              aggregatedMetrics[displayName][time] = value;
            }
          }
        }
      }
    }

    var rows = [];
    for (var name in aggregatedMetrics) {
      if (aggregatedMetrics.hasOwnProperty(name)) {
        var row = [];
        row.push(name);
        for (var i = 0; i < timeRanges.length; i++) {
          var time = timeRanges[i];
          row.push(Number(Number(aggregatedMetrics[name][time] || 0).toFixed(2)));
        }
        rows.push(row);
      }
    }
    return rows;
  },

  render: function () {
    if (this.props.info.comp_name) {
      // A component is selected, return empty div.
      return (<div id="topologycounters"></div>)
    }
    var title = this.getTitle();
    var rows = this.getTopologyMetricsRows();

    var timeRanges = ["10 mins", "1 hr", "3 hrs", "All Time"];
    var headings = ["Metrics"];
    headings.push.apply(headings, timeRanges);

    return (
      <div id="topologycounters">
        <div className="widget-header">
          <div className="title">
            <h4 style={{
              "display": "inline-block",
              "float": "left",
              "margin-right": "10px"
            }}>{title}</h4>
            <div style={{
              "padding-top": "10px",
              "padding-bottom": "10px",
            }}>
            </div>
          </div>
        </div>
        <table className="table table-striped table-hover no-margin">
          <thead>
            <tr>
              {headings.map(function (heading, i) {
                  return <th key={i}>{heading}</th>;
              })}
            </tr>
          </thead>
          <tbody>
            {rows.map(function (row) {
              return <tr key={row[0]}>{
                row.map(function (value, i) {
                  return <td className="col-md-2" key={i}>{value}</td>;
                })}</tr>;
            })}
          </tbody>
        </table>
      </div>
    );
  }
});

/*
 * This section contains key running information for all spouts.
 * It is visible only when no component is selected.
 */
var SpoutRunningInfo = React.createClass({
  capitalize: function(astr) {
    if (astr) {
        return astr.charAt(0).toUpperCase() + astr.slice(1);
    }
    return undefined;
  },

  getTitle: function () {
    return "Spout Running Info: " + this.state.time;
  },

  getInitialState: function () {
    return {
      sortBy: 0,
      reverse: false,
      time: "10 mins"  // 10 mins metrics is used.
    };
  },

  getSpoutRows: function (time) {
    const metrics = this.props.info.metrics;
    const metricNames = ["Emit Count", "Ack Count"];
    var aggregatedMetrics = {};
    var spoutNames = [];

    // Get spout names.
    var spouts = this.props.info.pplan.spouts;
    for (var name in spouts) {
      if (spouts.hasOwnProperty(name)) {
        spoutNames.push(name);
      }
    }

    for (var s = 0; s < spoutNames.length; s++) {
      var spoutName = spoutNames[s];
      // Init results to "_"
      aggregatedMetrics[spoutName] = {};
      for (var i in metricNames) {
        var metricName = metricNames[i];
        aggregatedMetrics[spoutName][metricName] = "_";
      }
      // Aggregate
      if (metrics[spoutName].hasOwnProperty(time)) {
        // For all metrics
        for (var i in metricNames) {
          var metricName = metricNames[i];
          var count = 0;
          var sum = 0;
          if (metrics[spoutName][time].hasOwnProperty(metricName)) {
            // For all streams/instances
            for (var stream in metrics[spoutName][time][metricName]) {
              if (metrics[spoutName][time][metricName].hasOwnProperty(stream)) {
                var allMetrics = metrics[spoutName][time][metricName][stream].metrics;
                for (var m in allMetrics) {
                  if (allMetrics.hasOwnProperty(m)) {
                    var v = Number(allMetrics[m]) / (metrics[spoutName][time][metricName][stream].scaleDevisor || 1);
                    count++;
                    sum += v;
                  }
                }
              }
            }
          }
          if (count > 0) {
            aggregatedMetrics[spoutName][metricName] = sum;
          }
        }
      }
    }

    var rows = [];
    for (var id in spoutNames) {
      var spoutName = spoutNames[id];
      var row = [];
      row.push(spoutName);
      // Put parallelism
      var spouts = this.props.info.lplan.spouts;
      var parallelism = spouts[spoutName]["config"][ParallelismConfig];
      row.push(parallelism);

      // Put metrics
      for (var i in metricNames) {
        var metricName = metricNames[i];
        row.push(aggregatedMetrics[spoutName][metricName]);
      }

      rows.push(row);
    }
    return rows;
  },

  render: function () {
    if (this.props.info.comp_name) {
      // A component is selected, return empty div.
      return (<div id="spoutrunninginfo"></div>)
    }
    var title = this.getTitle();
    var rows = this.getSpoutRows(this.state.time);

    var reverse = this.state.reverse;
    var sortKey = this.state.sortBy;
    rows.sort(function (a, b) {
      var aVal = a[sortKey];
      var bVal = b[sortKey];
      return (typeof aVal === "string" ? aVal.localeCompare(bVal) : (bVal - aVal)) * (reverse ? 1 : -1);
    });
    var setState = this.setState.bind(this);

    const headings = ["Spout", "Parallelism", "Emit Count", "Ack Count"];

    return (
      <div id="spoutrunninginfo">
        <div className="widget-header">
          <div className="title">
            <h4 style={{
              "display": "inline-block",
              "float": "left",
              "margin-right": "10px"
            }}>{title}</h4>
            <div style={{
              "padding-top": "10px",
              "padding-bottom": "10px",
            }}>
            </div>
          </div>
        </div>
        <table className="table table-striped table-hover no-margin">
          <thead>
            <tr>
              {headings.map(function (heading, i) {
                var classNameVals = [
                  'sort',
                  ((sortKey === i) && reverse) ? 'asc' : '',
                  ((sortKey === i) && !reverse) ? 'desc' : ''
                ].join(' ');
                function clicked() {
                  setState({
                    sortBy: i,
                    reverse: i === sortKey ? (!reverse) : true
                  });
                }
                return <th key={i} className={classNameVals} onClick={clicked}>{heading}</th>;
              })}
            </tr>
          </thead>
          <tbody>
            {rows.map(function (row) {
              return <tr key={row[0]}>{
                row.map(function (value, i) {
                  return <td className="col-md-2" key={i}>{value}</td>;
                })}</tr>;
            })}
          </tbody>
        </table>
      </div>
    );
  }
});

/*
 * This section contains key running information for all bolts.
 * It is visible only when no component is selected.
 */
var BoltRunningInfo = React.createClass({
  capitalize: function(astr) {
    if (astr) {
        return astr.charAt(0).toUpperCase() + astr.slice(1);
    }
    return undefined;
  },

  getTitle: function () {
    return "Bolt Running Info: " + this.state.time;
  },

  getInitialState: function () {
    return {
      sortBy: 0,
      reverse: false,
      time: "10 mins"  // 10 mins metrics is used.
    };
  },

  getBoltRows: function (time) {
    const metrics = this.props.info.metrics;
    const metricNames = ["Execute Count", "Fail Count"];
    const minUtilizationName = "Min Capacity Utilization";
    const maxUtilizationName = "Max Capacity Utilization";
    var aggregatedMetrics = {};
    var boltNames = [];

    // Get bolt names.
    var bolts = this.props.info.pplan.bolts;
    for (var name in bolts) {
      if (bolts.hasOwnProperty(name)) {
        boltNames.push(name);
      }
    }

    for (var b = 0; b < boltNames.length; b++) {
      var boltName = boltNames[b];
      // Init results to "_"
      aggregatedMetrics[boltName] = {};
      for (var i in metricNames) {
        var metricName = metricNames[i];
        aggregatedMetrics[boltName][metricName] = "_";
      }
      aggregatedMetrics[boltName][minUtilizationName] = "_";
      aggregatedMetrics[boltName][maxUtilizationName] = "_";

      // Aggregate
      if (metrics[boltName].hasOwnProperty(time)) {
        // For all metrics
        for (var i in metricNames) {
          var metricName = metricNames[i];
          var count = 0;
          var sum = 0;
          if (metrics[boltName][time].hasOwnProperty(metricName)) {
            // For all streams/instances
            for (var stream in metrics[boltName][time][metricName]) {
              if (metrics[boltName][time][metricName].hasOwnProperty(stream)) {
                var allMetrics = metrics[boltName][time][metricName][stream].metrics;
                for (var m in allMetrics) {
                  if (allMetrics.hasOwnProperty(m)) {
                    var v = Number(allMetrics[m]) / (metrics[boltName][time][metricName][stream].scaleDevisor || 1);
                    count++;
                    sum += v;
                  }
                }
              }
            }
          }
          if (count > 0) {
            aggregatedMetrics[boltName][metricName] = sum;
          }
        }
        // For capacity utilization. Manually calculate it by execution count times latency.
        const executeCountMetricName = "Execute Count";
        const executeLatencyMetricName = "Execute Latency (ms)";
        if (metrics[boltName][time].hasOwnProperty(executeCountMetricName) &&
            metrics[boltName][time].hasOwnProperty(executeLatencyMetricName)) {
          // For all streams/instances
          var instanceUtilization = {};  // milliseconds spend on execution, in the window.
          for (var stream in metrics[boltName][time][executeCountMetricName]) {
            if (metrics[boltName][time][executeCountMetricName].hasOwnProperty(stream) &&
                metrics[boltName][time][executeLatencyMetricName].hasOwnProperty(stream)) {
              var countMetrics = metrics[boltName][time][executeCountMetricName][stream].metrics;
              var latencyMetrics = metrics[boltName][time][executeLatencyMetricName][stream].metrics;
              // For each intance
              for (var m in countMetrics) {
                if (countMetrics.hasOwnProperty(m) && latencyMetrics.hasOwnProperty(m)) {
                  var count = Number(countMetrics[m]) / (metrics[boltName][time][executeCountMetricName][stream].scaleDevisor || 1);
                  var latency = Number(latencyMetrics[m]) / (metrics[boltName][time][executeLatencyMetricName][stream].scaleDevisor || 1);
                  var utilization = count * latency;

                  instanceUtilization[m] = (instanceUtilization[m] || 0) + utilization;
                }
              }
            }
          }
          // Calculate min/max.
          var min = -1;
          var max = -1;
          for (var i in instanceUtilization) {
            if (instanceUtilization.hasOwnProperty(i)) {
              var utilization = instanceUtilization[i] * 100 / (10 * 60 * 1000);   // Divide by the time window and get percentage.
              if (min === -1 || min > utilization) {
                min = utilization;
              }
              if (max === -1 || max < utilization) {
                max = utilization;
              }
            }
          }

          if (min !== -1) {
            aggregatedMetrics[boltName][minUtilizationName] = min.toFixed(2) + "%";
            aggregatedMetrics[boltName][maxUtilizationName] = max.toFixed(2) + "%";
          }
        }
      }
    }

    var rows = [];
    for (var id in boltNames) {
      var boltName = boltNames[id];
      var row = [];
      row.push(boltName);
      // Put parallelism
      var bolts = this.props.info.lplan.bolts;
      var parallelism = bolts[boltName]["config"][ParallelismConfig];
      row.push(parallelism);

      // Put metrics
      for (var i in metricNames) {
        var metricName = metricNames[i];
        row.push(aggregatedMetrics[boltName][metricName]);
      }
      row.push(aggregatedMetrics[boltName][minUtilizationName]);
      row.push(aggregatedMetrics[boltName][maxUtilizationName]);

      rows.push(row);
    }
    return rows;
  },

  render: function () {
    if (this.props.info.comp_name) {
      // A component is selected, return empty div.
      return (<div id="boltrunninginfo"></div>)
    }
    var title = this.getTitle();
    var rows = this.getBoltRows(this.state.time);

    var reverse = this.state.reverse;
    var sortKey = this.state.sortBy;
    rows.sort(function (a, b) {
      var aVal = a[sortKey];
      var bVal = b[sortKey];
      return (typeof aVal === "string" ? aVal.localeCompare(bVal) : (bVal - aVal)) * (reverse ? 1 : -1);
    });
    var setState = this.setState.bind(this);

    const headings = [
      { name: "Bolt", sortable: true },
      { name: "Parallelism", sortable: true },
      { name: "Execute Count", sortable: true },
      { name: "Failure Count", sortable: true },
      { name: "Capacity Utilization(min)", sortable: true },
      { name: "Capacity Utilization(max)", sortable: true },
      { name: "Parallelism Calculator", sortable: false }
    ];

    var openParallelismCalculator = function (e, index) {
      var row = rows[index];

      var calculator = $("#parallelism-calculator-modal");
      calculator.find('#modal-component').text(row[0]);
      calculator.attr('data-execute-count', row[2]);
      calculator.find('#modal-execute-count').text(row[2]);
      calculator.find('#target-execute-count').val(row[2]);
      calculator.attr('data-max-utilization', row[5].replace("%", ""));
      calculator.find('#modal-max-utilization').text(row[5]);
      calculator.find('#target-max-utilization').val(row[5].replace("%", ""));
      calculator.attr('data-parallelism', row[1]);
      calculator.find('#modal-parallelism').text(row[1]);
      calculator.find('#target-parallelism').val(row[1]);

      calculator.modal({keyboard: true});
    };

    return (
      <div id="componentrunninginfo">
        <div className="widget-header">
          <div className="title">
            <h4 style={{
              "display": "inline-block",
              "float": "left",
              "margin-right": "10px"
            }}>{title}</h4>
            <div style={{
              "padding-top": "10px",
              "padding-bottom": "10px",
            }}>
            </div>
          </div>
        </div>
        <table className="table table-striped table-hover no-margin">
          <thead>
            <tr>
              {headings.map(function (heading, i) {
                var classNameVals = [
                  'sort',
                  ((sortKey === i) && reverse) ? 'asc' : '',
                  ((sortKey === i) && !reverse) ? 'desc' : ''
                ].join(' ');
                function clicked() {
                  setState({
                    sortBy: i,
                    reverse: i === sortKey ? (!reverse) : true
                  });
                }

                if (heading.sortable) {
                  return <th key={i} className={classNameVals} onClick={clicked}>{heading.name}</th>;
                } else {
                  return <th key={i}>{heading.name}</th>;
                }
              })}
            </tr>
          </thead>
          <tbody>
            {rows.map(function (row, index) {
              return <tr key={row[0]}>{
                  row.map(function (value, i) {
                    return <td className="col-md-2" key={i}>{value}</td>;
                  })}
                  <td className="col-md-1">
                    <button type="button" className="btn btn-primary btn-xs" data-toggle="modal"
                            onClick={(e) => openParallelismCalculator(e, index)}>
                      Calculator
                    </button>
                  </td></tr>;
            })}
          </tbody>
        </table>
      </div>
    );
  }
});

var ComponentCounters = React.createClass({
  capitalize: function(astr) {
    if (astr) {
        return astr.charAt(0).toUpperCase() + astr.slice(1);
    }
    return undefined;
  },

  getTitle: function () {
    if (this.props.info.comp_name) {
      var comp_type = this.capitalize(this.props.info.comp_type);
      var title = " " + comp_type + " Counters - " + this.props.info.comp_name;
      if (this.props.info.comp_spout_type !== undefined
          && this.props.info.comp_spout_type !== "default") {
        title += " - " + this.props.info.comp_spout_type + "/" + this.props.info.comp_spout_source;
      }
      return title;
    }
    return "";
  },

  getComponentMetricsRows: function () {
    var metrics = {};
    if (this.props.info.metrics.hasOwnProperty(this.props.info.comp_name)) {
      metrics = this.props.info.metrics[this.props.info.comp_name];
    }
    var aggregatedMetrics = {};
    var timeRanges = ["10 mins", "1 hr", "3 hrs", "All Time"];

    for (var time in metrics) {
      if (metrics.hasOwnProperty(time)) {
        for (var metricname in metrics[time]) {
          if (metrics[time].hasOwnProperty(metricname) &&
              metricname !== "__interval" &&
              metricname !== "Uptime (ddd:hh:mm:ss)") {
            for (var stream in metrics[time][metricname]) {
              if (metrics[time][metricname].hasOwnProperty(stream)) {
                displayName = metricname + " (" + stream + ")";
                var value = 0;
                var allMetrics = metrics[time][metricname][stream].metrics;
                var aggregationType = metrics[time][metricname][stream].aggregationType;
                var count = 0;
                for (var m in allMetrics) {
                  if (allMetrics.hasOwnProperty(m)) {
                    value += Number(allMetrics[m]);
                    count++;
                  }
                }
                if (aggregationType === AVG && count > 0) {
                  value /= count;
                }
                if (!aggregatedMetrics.hasOwnProperty(displayName)) {
                  aggregatedMetrics[displayName] = {};
                }
                value /= (metrics[time][metricname][stream].scaleDevisor || 1);
                aggregatedMetrics[displayName][time] = value;
              }
            }
          }
        }
      }
    }

    var rows = [];
    for (var name in aggregatedMetrics) {
      if (aggregatedMetrics.hasOwnProperty(name)) {
        var row = [];
        row.push(name);
        for (var i = 0; i < timeRanges.length; i++) {
          var time = timeRanges[i];
          row.push(Number(Number(aggregatedMetrics[name][time] || 0).toFixed(2)));
        }
        rows.push(row);
      }
    }
    return rows;
  },

  render: function () {
    if (!this.props.info.comp_name) {
      // No component is selected, return empty div.
      return (<div id="componentcounters"></div>)
    }

    var title = this.getTitle();
    var rows = this.getComponentMetricsRows();

    var timeRanges = ["10 mins", "1 hr", "3 hrs", "All Time"];
    var headings = ["Metrics"];
    headings.push.apply(headings, timeRanges);

    var extraLinks = [];
    var spoutDetail = this.props.info.lplan.spouts[this.props.info.comp_name];
    if (spoutDetail) {
      extraLinks = spoutDetail.extra_links;
    }

    return (
      <div id="componentcounters">
        <div className="widget-header">
          <div className="title">
            <h4 style={{
              "display": "inline-block",
              "float": "left",
              "margin-right": "10px"
            }}>{title}</h4>
            <div style={{
              "padding-top": "10px",
              "padding-bottom": "10px",
            }}>
            {extraLinks.map(function (extraLink) {
              return <a id={extraLink['name']}
                        className="btn btn-primary btn-xs"
                        href={extraLink['url']}
                        target="_blank"
                        style={{"margin-right": "5px"}}>{extraLink['name']}
                     </a>
            })}
            </div>
          </div>
        </div>
        <table className="table table-striped table-hover no-margin">
          <thead>
            <tr>
              {headings.map(function (heading, i) {
                  return <th key={i}>{heading}</th>;
              })}
            </tr>
          </thead>
          <tbody>
            {rows.map(function (row) {
              return <tr key={row[0]}>{
                row.map(function (value, i) {
                  return <td className="col-md-2" key={i}>{value}</td>;
                })}</tr>;
            })}
          </tbody>
        </table>
      </div>
    );
  }
});

var InstanceCounters = React.createClass({
  capitalize: function(astr) {
    if (astr) {
        return astr.charAt(0).toUpperCase() + astr.slice(1);
    }
    return undefined;
  },

  getInitialState: function () {
    return {
      sortBy: 0,
      reverse: false,
      maxRows: 50
    };
  },

  render: function () {
    if (!this.props.info.comp_name) {
      // No component is selected, return empty div.
      return <div id="instancecounters"></div>;
    }
    var self = this;
    var metrics = {};
    if (this.props.info.metrics.hasOwnProperty(this.props.info.comp_name)) {
      metrics = this.props.info.metrics[this.props.info.comp_name]["All Time"];
    }
    var comp_type = this.capitalize(this.props.info.comp_type);
    var pplan = this.props.info.pplan;
    var title = " " + comp_type + " Instance Counters - " + this.props.info.comp_name;

    var headings = ["Instances"];
    var metricNames = [];
    var aggregatedMetrics = {};

    // The comp_type is either "spout" or "bolt".
    // Just append a "s" and we can query pplan for that type.
    var instances = pplan[this.props.info.comp_type + "s"][this.props.info.comp_name];

    if (this.props.info.comp_type === "bolt") {
      tenMinMetrics = this.props.info.metrics[this.props.info.comp_name]["10 mins"];
    }

    for (var metricname in metrics) {
      if (metrics.hasOwnProperty(metricname)
          && metricname !== "__interval") {
        metricNames.push(metricname);
        if (!aggregatedMetrics.hasOwnProperty(metricname)) {
          aggregatedMetrics[metricname] = {};
        }
        var value = 0;
        var numStreams = 0;
        // Handle __jvm-uptime-secs separately.
        if (metricname === "Uptime (ddd:hh:mm:ss)") {
          var allMetrics = metrics[metricname][""].metrics;
          for (var m in allMetrics) {
            if (allMetrics.hasOwnProperty(m)) {
              if (instances.indexOf(m) < 0) {
                instances.push(m);
              }
              var secs = Number(allMetrics[m]);
              var ddd = ("000" + Number(secs / (24*60*60)).toFixed(0)).slice(-3);
              secs = secs % (24*60*60);
              var hh = ("00" + Number(secs / (60*60)).toFixed(0)).slice(-2);
              secs = secs % (60*60);
              var mm = ("00" + Number(secs / 60).toFixed(0)).slice(-2);
              ss = ("00" + (secs % 60)).slice(-2);
              aggregatedMetrics[metricname][m] =
                ddd + ":" +
                hh + ":" +
                mm + ":" +
                ss;
            }
          }
        } else {
          for (var stream in metrics[metricname]) {
            if (metrics[metricname].hasOwnProperty(stream)) {
              numStreams++;
              var allMetrics = metrics[metricname][stream].metrics;
              var aggregationType = metrics[metricname][stream].aggregationType;
              for (var m in allMetrics) {
                if (allMetrics.hasOwnProperty(m)) {
                  if (instances.indexOf(m) < 0) {
                    instances.push(m);
                  }
                  if (!aggregatedMetrics[metricname].hasOwnProperty(m)) {
                    aggregatedMetrics[metricname][m] = 0;
                  }
                  aggregatedMetrics[metricname][m] += Number(allMetrics[m]) /
                    (metrics[metricname][stream].scaleDevisor || 1);;
                }
              }
            }
          }
          if (aggregationType === AVG && numStreams > 0) {
            for (var m in aggregatedMetrics[metricname]) {
              if (aggregatedMetrics[metricname].hasOwnProperty(m)) {
                aggregatedMetrics[metricname][m] /= numStreams;
              }
            }
          }
        }
      }
    }

    if (this.props.info.comp_type === "bolt") {
      // Hacky way to calculate last 10 mins capacity
      var tenMinAggregatedMetrics = {};
      for (var metricname in tenMinMetrics) {
        if (tenMinMetrics.hasOwnProperty(metricname)
            && (metricname === "Execute Count"
                || metricname === "Execute Latency (ms)")) {
          if (!tenMinAggregatedMetrics.hasOwnProperty(metricname)) {
            tenMinAggregatedMetrics[metricname] = {};
          }
          var value = 0;
          var numStreams = 0;
          for (var stream in tenMinMetrics[metricname]) {
            if (tenMinMetrics[metricname].hasOwnProperty(stream)) {
              numStreams++;
              var allMetrics = tenMinMetrics[metricname][stream].metrics;
              var aggregationType = tenMinMetrics[metricname][stream].aggregationType;
              for (var m in allMetrics) {
                if (allMetrics.hasOwnProperty(m)) {
                  if (instances.indexOf(m) < 0) {
                    instances.push(m);
                  }
                  if (!tenMinAggregatedMetrics[metricname].hasOwnProperty(m)) {
                    tenMinAggregatedMetrics[metricname][m] = 0;
                  }
                  tenMinAggregatedMetrics[metricname][m] += Number(allMetrics[m]) /
                    (tenMinMetrics[metricname][stream].scaleDevisor || 1);;
                }
              }
            }
          }
          if (aggregationType === AVG && numStreams > 0) {
            for (var m in tenMinAggregatedMetrics[metricname]) {
              if (tenMinAggregatedMetrics[metricname].hasOwnProperty(m)) {
                tenMinAggregatedMetrics[metricname][m] /= numStreams;
              }
            }
          }
        }
      }
    }

    headings.push.apply(headings, metricNames);
    if (this.props.info.comp_type === "bolt") {
      headings.push("Capacity (last 10 mins)");
    }
    headings.push("");

    var rows = [];
    for (var i=0; i<instances.length; i++) {
      var instance = instances[i];
      var row = [];
      row.push(instance);
      for (var m = 0; m < metricNames.length; m++) {
        var name = metricNames[m];
        if (name === "Uptime (ddd:hh:mm:ss)") {
          row.push(aggregatedMetrics[name][instance]|| "000:00:00:00");
        } else {
          row.push(Number(Number(aggregatedMetrics[name][instance] || 0).toFixed(2)));
        }
      }
      if (this.props.info.comp_type === "bolt") {
        var capacity = (Number(tenMinAggregatedMetrics["Execute Count"][instance]));
        capacity *= (Number(tenMinAggregatedMetrics["Execute Latency (ms)"][instance]));
        capacity = capacity * 100 / (10 * 60 * 1000);
        row.push(Number((capacity.toFixed(2)) || 0) + "%");
      }
      if (pplan) {
        // Get Job url from pplan.
        var instanceInfo = undefined;
        for (var key in pplan.instances) {
          var instInfo = pplan.instances[key];
          if (instInfo.id === instance) {
            instanceInfo = instInfo;
            break;
          }
        }
        if (instanceInfo) {
          var stmgrId = instanceInfo.stmgrId;
          var container = stmgrId.split("-")[1]
          var logfileUrl = this.props.info.baseUrl + '/topologies/' + this.props.info.cluster
              + '/' + this.props.info.environ + '/' + this.props.info.topology
              + '/' + container + '/file?path=./log-files/' + instanceInfo.id + '.log.0'
          var jobUrl = this.props.info.baseUrl + '/topologies/filestats/' + this.props.info.cluster
              + '/' + this.props.info.environ + '/' + this.props.info.topology
              + '/' + container;
          var exceptionsUrl = this.props.info.baseUrl + '/topologies/' + this.props.info.cluster
              + '/' + this.props.info.environ + '/' + this.props.info.topology
              + '/' + this.props.info.comp_name + '/' + instance + '/exceptions';
          var pidUrl = this.props.info.baseUrl + '/topologies/' + this.props.info.cluster
              + '/' + this.props.info.environ + '/' + this.props.info.topology
              + '/' + instanceInfo.id + '/pid'
          var jstackUrl = this.props.info.baseUrl + '/topologies/' + this.props.info.cluster
              + '/' + this.props.info.environ + '/' + this.props.info.topology
              + '/' + instanceInfo.id + '/jstack'
          var jmapUrl = this.props.info.baseUrl + '/topologies/' + this.props.info.cluster
              + '/' + this.props.info.environ + '/' + this.props.info.topology
              + '/' + instanceInfo.id + '/jmap'
          var histoUrl = this.props.info.baseUrl + '/topologies/' + this.props.info.cluster
              + '/' + this.props.info.environ + '/' + this.props.info.topology
              + '/' + instanceInfo.id + '/histo'
          var links = [['Logs', logfileUrl, "_blank"],
                       ['Job', jobUrl, "_blank"],
                       ['Exceptions', exceptionsUrl, "_self"],
                       ['Pid', pidUrl, "_self"],
                       ['Stack', jstackUrl, "_self"],
                       ['MemHistogram', histoUrl, "_self"],
                       ['MemoryDump', jmapUrl, "_self"]];

          // Make them links.
          var linkButton = (<ActionButton links={links}/>)
          row.push(linkButton);
        }
      }
      rows.push(row);
    }
    var headingSortClass = headings.map(function(heading, i) {
      return 'sort-' + i;
    });

    var instanceId = this.props.info.instance;
    var compName = this.props.info.comp_name;
    var reverse = this.state.reverse;
    var sortKey = this.state.sortBy;
    rows.sort(function (a, b) {
      var aVal = a[sortKey];
      var bVal = b[sortKey];
      return (typeof aVal === "string" ? aVal.localeCompare(bVal) : (bVal - aVal)) * (reverse ? 1 : -1);
    });
    var setState = this.setState.bind(this);
    var maxRows = this.state.maxRows;
    var that = this;
    function incrLimit() {
      that.setState({
        maxRows: maxRows + 50
      });
    }
    // only show "show more" button if we are hiding rows
    var showMore;
    if (rows.length - 1 >= maxRows) {
      showMore = <div className="text-center" onClick={incrLimit}>
        <strong>{rows.length - maxRows} instances omitted.  Click for more.</strong>
      </div>;
    } else {
      showMore = null;
    }

    extraRowStyle = {
      minWidth: '100px',
    }
    return (
      <div id="instancecounters">
        <hr/>
        <div className="widget-header">
          <div className="title">
            <h4>{title}</h4>
          </div>
        </div>
        <table className="table table-striped table-hover no-margin">
          <thead>
            <tr>
              {headings.map(function (heading, i) {
                var classNameVals = [
                  'sort',
                  ((sortKey === i) && reverse) ? 'asc' : '',
                  ((sortKey === i) && !reverse) ? 'desc' : ''
                ].join(' ');
                function clicked() {
                  setState({
                    sortBy: i,
                    reverse: i === sortKey ? (!reverse) : true
                  });
                }
                return <th key={i} className={classNameVals} style={extraRowStyle} onClick={clicked}>{heading}</th>;
              })}
            </tr>
          </thead>
          <tbody className="list">
            {rows.slice(0, maxRows).map(function (row) {
              var highlighted = row[0] === instanceId;
              return <InstanceRow key={row[0]} row={row} headings={headingSortClass} highlighted={highlighted}/>;
            })}
          </tbody>
        </table>
        {showMore}
      </div>
    );
  }
});
