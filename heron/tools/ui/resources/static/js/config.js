/** @jsx React.DOM */

var ConfigTable = React.createClass({
  getInitialState: function() {
    return {'config': {}};
  },
  getTopologyConfig: function() {
    urlTokens = ['/topologies',
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
        if (response.hasOwnProperty('result')
            && response.result.hasOwnProperty('config')) {
          this.setState({config: response.result.config});
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
    this.getTopologyConfig();
  },
  componentDidUpdate: function(prevProps, prevState) {
    if (prevProps.topology != this.props.topology) {
      this.getTopologyConfig();
    }
  },
  render: function() {
    var configData = this.state.config;
    var headings = ['Property', 'Value'];
    var title = 'Configuration';
    var rows = [];
    var self = this;
    for (property in configData) {
      var configValue = configData[property];
      if (typeof configValue == 'string') {
        rows.push([property, configValue]);
      } else {
        rows.push([property, JSON.parse(configValue.value)]);
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
      <div>
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
