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

// TODO(nbhagat): Aggregate exceptions for better display.
// Requires baseUrl, cluster, environ, topology, comp_name, instance properties.
var InstanceExceptionLogs = React.createClass({
  getInitialState: function() {
    return {
        exceptions: undefined,
    };
  },

  componentWillMount: function(prevProps, prevState) {
    // Use Ajax only in here.
    this.fetchPplan();
    if(this.props.comp_name === "All") {
      this.fetchAllComponentException(this.props.baseUrl, this.props.cluster, this.props.environ, this.props.topology);
    } else {
      this.fetchExceptions(this.props.baseUrl, this.props.cluster, this.props.environ, this.props.topology, this.props.comp_name);
    }
  },

  fetchPplan: function () {
    console.log("PPlan", this.props.baseUrl);
    url = this.props.baseUrl + "/topologies/" +
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
      }.bind(this),
    });
  },

  fetchAllComponentException: function (baseUrl, cluster, environ, topology) {
    var fetchUrl = [baseUrl, 'topologies', cluster, environ, topology, 'logicalplan.json'].join("/");
    console.log(baseUrl);
    console.log(fetchUrl);
    $.ajax({
      url: fetchUrl,
      dataType: 'json',
      success: function (response) {
        compNames = [];
        this.allExceptions = {}
        for (var spoutName in response.result.spouts) {
          compNames.push(spoutName);
        }
        for (var boltName in response.result.bolts) {
          compNames.push(boltName);
        }
        for (var i in compNames) {
          this.fetchExceptions(baseUrl, cluster, environ, topology, compNames[i]);
        }
        this.setState({"compNames": compNames})
      }.bind(this),
    });
  },

  fetchExceptions: function(baseUrl, cluster, environ, topology, compName) {
    var urlTokens = [baseUrl, 'topologies', cluster, environ, topology, compName, 'exceptions.json'];
    var fetchUrl = urlTokens.join("/");
    console.log(baseUrl);
    console.log(fetchUrl);
    $.ajax({
      url: fetchUrl,
      dataType:  'json',
      success: function(response) {
        if (response.hasOwnProperty("result")) {
          if (!this.state.exceptions) {
            this.setState({exceptions: response.result});
          } else {
            this.setState({exceptions: this.state.exceptions.concat(response.result)});
          }
        }
      }.bind(this),

      error: function() {
        alert('error getting posts. please try again later');
      }
    });
  },

  // Aggregate exception with if hash of first two line are same. Returns the array of distinct exceptions
  // This function assumes that exception log is sorted by time (i.e. most recent exception at the end).
  aggregateExceptions: function(exceptionLogs, filterId) {
    var uniqExceptions = {};

    for (i = 0; i < exceptionLogs.length; ++i) {
      var trace = exceptionLogs[i].stacktrace;
      var tokens = trace.split("\n", 3);
      if (filterId != 'All' && filterId != exceptionLogs[i].instance_id) {
        continue;
      }

      if (tokens.length == 3) {
        tokens.pop();  // Remove exverything but first two lines
      }
      var key = filterId + "\n" + tokens.join("\n");
      if (!(key in uniqExceptions)) {
        var exceptionStyle = {
          fontSize: '80%',
        }
        uniqExceptions[key] = {'instance': exceptionLogs[i].instance_id,
                               'stacktrace': (<pre style={exceptionStyle}>{trace}</pre>),
                               'count': parseFloat(exceptionLogs[i].count),
                               'firsttime': exceptionLogs[i].firsttime,
                               'lasttime': exceptionLogs[i].lasttime,
                               'logging' : exceptionLogs[i].logging,
                              };
      } else {
        var valueObj = uniqExceptions[key];
        valueObj.lasttime = exceptionLogs[i].lasttime;
        valueObj.count += parseFloat(exceptionLogs[i].count);
      }
    }
    var values = Object.keys(uniqExceptions).map(function(key) {
      return uniqExceptions[key];
    });
    return values;
  },

  render: function() {
    var exceptionLogs = [];
    if (this.state.exceptions) {
      exceptionLogs = this.aggregateExceptions(this.state.exceptions, this.props.instance);
    }

    var title = 'Recent exceptions for ' + this.props.instance;
    var headings = ["Trace", "Instance", "Oldest Record", "Latest Record", "Count", ""];
    var exceptions = [];
    for (i = 0; i < exceptionLogs.length; ++i) {
      var exceptionsUrl = this.props.baseUrl + '/topologies/' + this.props.cluster 
        + '/' + this.props.environ + '/' + this.props.topology
        + '/' + this.props.comp_name + '/' + exceptionLogs[i].instance
        + '/exceptions';
      var mainLinks = [['Exceptions', exceptionsUrl]];
      pplan = this.state.pplan;
      var instanceInfo = undefined;
      if (pplan) {
        // Get JobUrl and logfile url from pplan.
        for (var key in pplan.instances) {
          var instInfo = pplan.instances[key];
          if (instInfo.id === exceptionLogs[i].instance) {
            instanceInfo = instInfo;
            break;
          }
        }
        if (instanceInfo) {
          var logfile = instanceInfo.logfile;
          var stmgr_id = instanceInfo.stmgr_id;
          var jobUrl = pplan.stmgrs[stmgr_id].joburl;
          var host = "http://" + pplan.stmgrs[stmgr_id].host + ":1338";
          mainLinks = mainLinks.concat([['Logs', logfile], ['Aurora', jobUrl], ['Host', host]]);
        }
      }
      row = [ exceptionLogs[i].stacktrace,
              exceptionLogs[i].instance_id,
              exceptionLogs[i].firsttime,
              exceptionLogs[i].lasttime,
              exceptionLogs[i].count,
              <ActionButton links={mainLinks}/>];
      exceptions.push(row);
    }
    var extraRowStyle={
      minWidth: '100px',
    }
    var headingSortClass = headings.map(function(heading, i) {
      return 'sort-' + i;
    });
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
                {headings.map(function (heading) {
                    return <th style={extraRowStyle}>{heading}</th>;
                })}
              </tr>
            </thead>
            <tbody> {
              exceptions.map(
                function (exceptionRow) {
                  return <InstanceRow row={exceptionRow} headings={headingSortClass}/>
                  })
            }</tbody>
          </table>
        </div>
      );
  }
});

