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

var TopologyItem = React.createClass({
  render: function() {
    var topology = this.props.topology;
    var index = this.props.index;

    var divStyle = {
      position: 'relative',
      padding: '8px',
    };

    var topology = this.props.topology;
    var display_cluster = topology.cluster.toUpperCase();
    var display_env = topology.environ.toUpperCase();
    var display_instances = topology.instances;
    var display_time = "-";
    if (topology.submission_time !== "-") {
      display_time = moment(topology.submission_time * 1000).fromNow();
    }

    var state_class = "gradeX normal";
    if (!topology.has_tmanager_location) {
      state_class = "gradeX dead";
    } else if (!topology.has_physical_plan) {
      state_class = "gradeX weird";
    }

    var starting_duration = 5 * 60 * 1000; // 5 minutes
    if ((!topology.has_tmanager_location || !topology.has_physical_plan)
        && topology.submission_time * 1000 > new Date().getTime() - starting_duration) {
      state_class = "gradeX starting";
    }

    return (
       <tr className={state_class}>
         <td className="col-md-1 index no-break">{index}</td>
         <td className="col-md-3 break-all"><a className="topo_name" href={'./topologies/' + topology.cluster + '/' + topology.environ + '/' + topology.name}>{topology.name}</a></td>
         <td className="col-md-1 topo_status">{topology.status}</td>
         <td className="col-md-1 topo_cluster">{display_cluster}</td>
         <td className="col-md-1 topo_runrole break-all">{topology.role}</td>
         <td className="col-md-1 topo_environ">{display_env}</td>
         <td className="col-md-1 topo_instances">{display_instances}</td>
         <td className="col-md-2 topo_releaseversion break-all">{topology.release_version}</td>
         <td className="col-md-1 topo_submittedby break-all">{topology.submission_user}</td>
         <td className="col-md-2 topo_submittedat no-break">{display_time}</td>
       </tr>
    );
  }
});


var TopologyTable = React.createClass({
  getInitialState: function () {
    return { topologies: [], sortBy: "name" };
  },

  componentWillMount: function () {
    this.fetchTopologies();
  },

  fetchTopologies: function() {
    $.ajax({
      url:      './topologies/list.json',
      dataType: 'json',
      data:     { format: 'json' },
      success:  function (result) {
        topologies = [];
        for (var cluster in result) {
          for (var env in result[cluster]) {
            for (var topologyName in result[cluster][env]) {
              estate = result[cluster][env][topologyName];
              topologies.push({
                name: topologyName,
                cluster: estate.cluster,
                environ: env,
                role: estate.role,
                has_physical_plan: estate.has_physical_plan,
                has_tmanager_location: estate.has_tmanager_location,
                instances: estate.instances,
                release_version: estate.release_version,
                submission_time: estate.submission_time,
                submission_user: estate.submission_user,
                status: estate.status
              });
            }
          }
        }
        this.setState({ topologies: topologies });
      }.bind(this),

      error: function () {
      }
    });
  },

  render: function() {

    var linkHeaderStyle = {
      minWidth: '90px',
    }

    // split filter out into terms and make a regex for each term
    var filters = (this.props.filter || "").split(/\s+/).map(function (term) {
      return term.toLowerCase();
    });

    var topologies = this.state.topologies.filter(function(topo, i) {
      if (this.props.env == topo.environ || 'all' == this.props.env) {
        if (this.props.cluster == topo.cluster || 'all' == this.props.cluster) {
          // if every filter term is contained in some part of the topology
          var searchAgainst = _.values(topo).filter(_.isString).join(" ").toLowerCase();
          if (filters.every(function (f) { return searchAgainst.indexOf(f) !== -1; })) {
            return true;
          }
        }
      }
    }.bind(this));

    if (this.state.sortBy) {
      topologies = _.sortBy(topologies, this.state.sortBy);
      var neg = this.state.sortBy[0] === '-';
      var sortKey = neg ? this.state.sortBy.substr(1) : this.state.sortBy;
      topologies.sort(function (a, b) {
        var aVal = a[sortKey];
        var bVal = b[sortKey];
        return (typeof aVal === "string" ? aVal.localeCompare(bVal) : (bVal - aVal)) * (neg ? -1 : 1);
      });
    }

    var items = topologies.map(function (topo, index) {
      return (
        <TopologyItem topology={topo} index={index + 1}/>
      );
    });

    var sortClass = function (attr) {
      if (this.state.sortBy === attr) {
        return "sort asc";
      } else if (this.state.sortBy === "-" + attr) {
        return "sort desc";
      } else {
        return "sort";
      }
    }.bind(this);

    var sortBy = function (attr) {
      return function () {
        if (this.state.sortBy === attr) {
          attr = "-" + attr;
        }
        this.setState({sortBy: attr});
      }.bind(this);
    }.bind(this);

    return (
      <div>
        <div className="search-result">Found {items.length} topologies</div>
        <div className="table-responsive">
          <table className="table table-striped topotable">
            <thead>
              <th>
                Index
              </th>
              <th onClick={sortBy("name")} className={sortClass("name")}>
                Name
              </th>
              <th onClick={sortBy("status")} className={sortClass("status")}>
                Status
              </th>
              <th onClick={sortBy("cluster")} className={sortClass("cluster")}>
                Cluster
              </th>
              <th onClick={sortBy("role")} className={sortClass("role")}>
                Role
              </th>
              <th onClick={sortBy("environ")} className={sortClass("environ")}>
                Environ
              </th>
              <th onClick={sortBy("instances")} className={sortClass("instances")}>
                Instances
              </th>
              <th onClick={sortBy("release_version")} className={sortClass("release_version")}>
                Version
              </th>
              <th onClick={sortBy("submission_user")} className={sortClass("submission_user")}>
                Submitted by
              </th>
              <th onClick={sortBy("submission_time")} className={sortClass("submission_time")}>
                Launched at
              </th>
            </thead>

            <tbody className="list">{items}</tbody>
          </table>
        </div>
      </div>
    );
  }
});


var FilterableTopologyTable = React.createClass({

  // On initialization, register a handler to extract DC/env/filter from hash
  // when it changes due to browser navigation.  Changes to UI are put into the
  // hash and when the hash changes this listener pushes them into the component
  // state.
  componentDidMount: function () {
    $(window).on('hashchange', function () {
      var stateFromHash = this.getStateFromHash();
      this.setState(stateFromHash);
      // when the hash was changed programatically, don't update search box content
      if (!this.changeIsFromUs) {
        $('#search-box').val(stateFromHash.filter);
      }
      this.changeIsFromUs = false;
    }.bind(this));
  },

  // merge state changes into the hash
  setStateIntoHash: function (arg) {
    var state = _.extend(this.getStateFromHash(), arg);
    this.changeIsFromUs = true;
    window.location.hash = '/' + [state.cluster, state.environ, state.filter].map(encodeURIComponent).join("/");
  },

  // extract state from the hash
  getStateFromHash: function () {
    var hash = window.location.hash.substr(1);
    var parts = hash.split("/");
    return {
      cluster: decodeURIComponent(parts[1] || "all"),
      environ: decodeURIComponent(parts[2] || "all"),
      filter: decodeURIComponent(parts[3] || "")
    };
  },

  getInitialState: function () {
    return this.getStateFromHash();
  },

  handleEnvClick: function(event) {
    this.setStateIntoHash({
      environ: event.target.id
    });
    event.preventDefault();
  },

  handleDataCenterClick: function(event) {
    this.setStateIntoHash({
      cluster: event.target.id
    });
    event.preventDefault();
  },

  handleFilterChange: function(event) {
    // when user types into filter, update the hash parameter with 100ms debounce
    var val = event.target.value;
    clearTimeout(this.timeoutId);
    this.timeoutId = setTimeout(function () {
      this.setStateIntoHash({
        filter: val
      });
    }.bind(this), 300);
  },

  render: function() {
    var divStyle = {
      'margin-top': '20px'
    };

    var leftStyle = {
      'padding-right': '20px',
    };

    var clusterStyle = {
      'padding-left':  '5px',
      'padding-right': '5px'
    };

    var environStyle = {
      'padding-left':  '5px',
      'padding-right': '0px'
    };

    var brandStyle = {
      'padding-left':  '15px',
    };

    var clusters = [];
    this.props.clusters.forEach(function(cluster) {
      clusters.push(<li className=""> <a href="#" id={cluster} className={this.state.cluster == {cluster} ? 'active' : ''} onClick={this.handleDataCenterClick}>{cluster.toUpperCase()}</a></li>)
    }.bind(this));

  return (
   <div>
     <div className="row spacer">
       <div className="col-md-12" style={clusterStyle}>
         <div className="navbar-custom">
           <div className="navbar-header">
             <button type="button" className="navbar-toggle" data-toggle="collapse" data-target=".navbar-responsive-collapse">
               <span className="icon-bar"></span>
               <span className="icon-bar"></span>
               <span className="icon-bar"></span>
             </button>
             <a className="navbar-brand" style={brandStyle}>cluster</a>
           </div>

           <div className="navbar-collapse collapse navbar-responsive-collapse">
             <ul className="nav navbar-nav">
                <li className=""> <a href="#" id="all" className={this.state.cluster == "all" ? 'active' : ''} onClick={this.handleDataCenterClick}>ALL</a></li>
                {clusters}
             </ul>
           </div>
         </div>
       </div>


     </div>
     <input id="search-box" placeholder="Search for a topology" type="text" className="form-control col-md-7" style={divStyle} autoFocus={true} onChange={this.handleFilterChange} defaultValue={this.state.filter}/>
     <TopologyTable  env={this.state.environ} cluster={this.state.cluster} filter={this.state.filter}/>
   </div>
  )
 }
});
