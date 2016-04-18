/** @jsx React.DOM */

var SUM = 0,
    AVG = 1,
    LAST = 2;

var AllExceptions = React.createClass({
  getInitialState: function() {
    return {}
  },
  fetchExceptionSummary: function() {
    var compName = this.props.info.comp_name ? this.props.info.comp_name : 'All';
    var fetchUrl = '/topologies/' + this.props.info.cluster
        + '/' + this.props.info.environ + '/' + this.props.info.topology
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
    var allExceptionsUrl = '/topologies/' + this.props.info.cluster
        + '/' + this.props.info.environ + '/' + this.props.info.topology
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
      var fetch_url = "/topologies/" +
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
    url = "/topologies/" +
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
    var fetch_url = "";

    if (this.props.hasOwnProperty("comp_name")) {
      fetch_url = "/topologies/metrics?" +
        "cluster=" + this.props.cluster + "&" +
        "environ=" + this.props.environ + "&" +
        "topology=" + this.props.topology + "&" +
        "component=" + this.props.comp_name;
      var metricnames = [];

      // Explicitly add jvm uptime metric since it
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
      for (var i = 0; i < metricnames.length; i++) {
        for (var timeRange in this.timeRanges) {
          if (this.timeRanges.hasOwnProperty(timeRange)) {
            var url = fetch_url +
              "&metricname=" + metricnames[i] +
              "&interval=" + this.timeRanges[timeRange];
            this.fetchCountersURL(url, timeRange);
          }
        }
      }
    } else if (!this.props.hasOwnProperty("comp_name")) {
      // Fetch all metrics for all the spouts.
      // Must have pplan and metricnames to continue.
      if (this.state.pplan && this.state.lplan) {
        var spoutNames = [];
        for (var name in this.state.lplan.spouts) {
          if (this.state.lplan.spouts.hasOwnProperty(name)) {
            spoutNames.push(name);
          }
        }
        fetch_url = "/topologies/metrics?" +
          "cluster=" + this.props.cluster + "&" +
          "environ=" + this.props.environ + "&" +
          "topology=" + this.props.topology;
        for (var i = 0; i < spoutNames.length; i++) {
          var spout = spoutNames[i];
          var url = fetch_url + "&component=" + spout;
          for (var j = 0; j < this.state.lplan.spouts[spout].outputs.length; j++) {
            var streamName = this.state.lplan.spouts[spout].outputs[j].stream_name;
            for (var spoutMetric in this.spoutMetrics) {
              if (this.spoutMetrics.hasOwnProperty(spoutMetric)) {
                var metricname = spoutMetric + streamName;
                for (var timeRange in this.timeRanges) {
                  if (this.timeRanges.hasOwnProperty(timeRange)) {
                    var url = fetch_url +
                      "&component=" + spout +
                      "&metricname=" + metricname +
                      "&interval=" + this.timeRanges[timeRange];
                    this.fetchCountersURL(url, timeRange);
                  }
                }
              }
            }
          }
        }
      }
    }
  },

  fetchCountersURL: function(url, timeRange) {
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
      comp_type: this.props.comp_type,
      comp_name: this.props.comp_name,
      comp_spout_type: this.props.comp_spout_type,
      comp_spout_source: this.props.comp_spout_source,
      cluster: this.props.cluster,
      environ: this.props.environ,
      metrics: this.state.metrics,
      pplan: this.state.pplan,
      instance: this.props.instance,
      hoverOverInstance: this.props.hoverOverInstance,
      hoverOutInstance: this.props.hoverOutInstance
    };
    return (
      <div>
        <ComponentCounters info={info} />
        <InstanceCounters info={info} />
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
    return " Topology Counters";
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

  getTopologyMetricsRows: function () {
    var metrics = this.props.info.metrics;
    var aggregatedMetrics = {};
    var timeRanges = ["10 mins", "1 hr", "3 hrs", "All Time"];

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
    var title = this.getTitle();

    var timeRanges = ["10 mins", "1 hr", "3 hrs", "All Time"];
    var headings = ["Metrics"];
    headings.push.apply(headings, timeRanges);

    var rows = [];
    if (this.props.info.comp_name) {
      rows = this.getComponentMetricsRows();
    } else {
      rows = this.getTopologyMetricsRows();
    }

    return (
      <div>
        <div className="widget-header">
          <div className="title">
            <h4>{title}</h4>
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
      return <div></div>;
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
        capacity /= (10 * 60 * 1000);
        row.push(Number(capacity.toFixed(3)) || 0);
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
          var logfileUrl = '/topologies/' + this.props.info.cluster
              + '/' + this.props.info.environ + '/' + this.props.info.topology
              + '/' + container + '/file?path=./log-files/' + instanceInfo.id + '.log.0'
          var jobUrl = '/topologies/filestats/' + this.props.info.cluster
              + '/' + this.props.info.environ + '/' + this.props.info.topology
              + '/' + container;
          var exceptionsUrl = '/topologies/' + this.props.info.cluster
              + '/' + this.props.info.environ + '/' + this.props.info.topology
              + '/' + this.props.info.comp_name + '/' + instance + '/exceptions';
          var pidUrl = '/topologies/' + this.props.info.cluster
              + '/' + this.props.info.environ + '/' + this.props.info.topology
              + '/' + instanceInfo.id + '/pid'
          var jstackUrl = '/topologies/' + this.props.info.cluster
              + '/' + this.props.info.environ + '/' + this.props.info.topology
              + '/' + instanceInfo.id + '/jstack'
          var jmapUrl = '/topologies/' + this.props.info.cluster
              + '/' + this.props.info.environ + '/' + this.props.info.topology
              + '/' + instanceInfo.id + '/jmap'
          var histoUrl = '/topologies/' + this.props.info.cluster
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
    var hoverOverInstance = this.props.info.hoverOverInstance;
    var hoverOutInstance = this.props.info.hoverOutInstance;
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
              function hoverOver() {
                hoverOverInstance({id: row[0], name: compName });
              }
              return <InstanceRow key={row[0]} row={row} headings={headingSortClass} highlighted={highlighted} hoverOver={hoverOver} hoverOut={hoverOutInstance}/>;
            })}
          </tbody>
        </table>
        {showMore}
      </div>
    );
  }
});

