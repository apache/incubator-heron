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

var ConfigTable = React.createClass({
  getInitialState: function() {
    return {
      'config': {},
      'component_config': {}
    };
  },
  getConfig: function() {
    urlTokens = [  this.props.baseUrl,
                   'topologies',
                   this.props.cluster,
                   this.props.environ,
                   this.props.topology,
                   'physicalplan.json'];
    fetchUrl = urlTokens.join('/');
    console.log("url: " + fetchUrl);
    $.ajax({
      url: fetchUrl,
      dataType:  'json',
      success: function(response) {
        if (response.hasOwnProperty('result')) {
          // Topology config
          var config = {};
          if (response.result.hasOwnProperty('config')) {
            config = response.result.config;
          }
          // Component config
          var componentConfig = {};
          if (response.result.hasOwnProperty('components')) {
            for (var component in response.result.components) {
              componentConfig[component] = response.result.components[component].config;
            }
          }
          this.setState({
            config: config,
            component_config: componentConfig
          });
        }
      }.bind(this),

      error: function() {
        console.log('error getting posts. please try again later');
      }
    });
  },

  getDeepTable: function(name, value) {
    var self = this;
    var childRows = false;
    var rowStyle = {
      maxWidth: '800px',
      textAlign: 'left',
    };
    if (!value) {
      childRows = (<tr><td>{name}</td><td>nil</td></tr>);
    } else if (typeof value != 'object') {
      var displayText = value.toString();
      if (name.indexOf('base64') != -1) {
        try {
          displayText = window.atob(displayText);
        } catch (e) { }
      }
      childRows = (<tr><td>{name}</td><td style={rowStyle}>{displayText}</td></tr>);
    } else if (Object.keys(value).length == 0) {
      childRows = (<tr><td>{name}</td><td>[]</td></tr>);
    } else {
      var innerRows = false;
      var fields = Object.keys(value);
      innerRows = fields.map(function(fieldName) {
        var fieldValue = value[fieldName];
        if (fieldName == 'annotations' || fieldName == 'classdesc') {
          return null;
        }
        if (!fieldValue) {
          return (<tr><td>{fieldName}</td><td>nil</td></tr>);
        } else if (typeof fieldValue != 'object') {
          return (<tr><td>{fieldName}</td><td style={rowStyle}>{fieldValue.toString()}</td></tr>);
        } else if (Object.keys(fieldValue).length == 0) {
          return (<tr><td>{fieldName}</td><td>[]</td></tr>);
        } else {
          var contentData =  Object.keys(fieldValue).map(function(innerFieldName) {
            return innerFieldName + ':' + fieldValue[innerFieldName];
          });
          return (<tr><td>{fieldName}</td><td style={rowStyle}>{JSON.stringify(contentData)}</td></tr>);
        }
      });
      childRows = (
        <tr>
          <td>{name}</td>
          <td><table>{innerRows}</table></td>
        </tr>
      );
    }
    return childRows;
  },

  componentWillMount: function () {
    this.getConfig();
  },
  componentDidUpdate: function(prevProps, prevState) {
    if (prevProps.topology != this.props.topology) {
      this.getConfig();
    }
  },
  render: function() {
    var configData = this.state.config;
    var componentConfigData = this.state.component_config;
    var headings = ['Property', 'Value'];
    var title = 'Configuration';
    var rows = [];
    var self = this;
    // Fill topology config data
    for (property in configData) {
      var configValue = configData[property];
      if (typeof configValue == 'object') {
        rows.push([property, JSON.parse(configValue.value)]);
      } else {
        rows.push([property, configValue]);
      }
    }
    // Fill component config data
    for (component in componentConfigData) {
      data = componentConfigData[component];
      for (property in data) {
        var key = '[' + component + '] ' + property;
        var configValue = data[property];
        if (typeof configValue == 'object') {
          rows.push([key, JSON.parse(configValue.value)]);
        } else {
          rows.push([key, configValue]);
        }
      }
    }

    var tableContent = ( 
      <tbody>{
        rows.map(function(row) {
          return self.getDeepTable(row[0], row[1]);
        })
      }</tbody>
    );

    var tableStyle = {
      wordWrap: 'break-word',
      tableLayout: 'inherit'
    };
    return (
      <div className="display-info display-config">
        <div className="widget-header">
          <div className="title">
            <h4>{title}</h4>
          </div>
        </div>
        <table style={tableStyle} className="table table-striped table-hover no-margin">
          <thead>
            <tr>
              {
                headings.map(function (heading) {
                  return <th>{heading}</th>;
                })
              }
            </tr>
          </thead>
          {tableContent}
        </table>
      </div>
    );
  }
});
