/** @jsx React.DOM */

var ActionButton = React.createClass({
  render: function() {
    // Map containing key to destination links.
    var buttonDivStyle = {
      position: 'relative',
      marginTop: '-1px',
      marginLeft: '8px',
      float: 'right',
    };
    var buttonStyle = this.props.buttonStyle ? this.props.buttonStyle : {}
    var textStyle = {
      textAlign: 'left',
    }
    var caretStyle = {
      marginLeft: '4px'
    }
    var dropdownStyle = {
      left: 'auto',
      right: '0'
    }
    return (
      <div className="btn-group action-button-group visible-on-hover" style={buttonDivStyle}>
        <button type="button" className="btn btn-xs btn-default dropdown-toggle" style={buttonStyle} data-toggle="dropdown">
          View
          <span className="caret" style={caretStyle}></span>
          <span className="sr-only">Toggle Dropdown</span>
        </button>
        <ul className="dropdown-menu" role="menu" style={dropdownStyle}>
        {
          this.props.links.map(function(linkData, i) {
            return (<li key={i}><a href={linkData[1]} target={linkData[2]} style={textStyle}>{linkData[0]}</a></li>)
          })
        }
        </ul>
      </div>
    );
  }
});

var InstanceRow = React.createClass({
  getInitialState: function () {
    return { showLinks: true };
  },
  render: function() {
    var self = this;
    var row = this.props.row;
    var headings = this.props.headings;
    var className = this.props.highlighted ? 'strong' : '';
    if (!this.state.showLinks) {
      row = row.slice(0, row.length - 1);
      row.push('');
    }
    var rowStyle = {
      'height': '35px',
    }
    return (
      <tr className={className}>{
      row.map(
        function (value, i) {
          return <td key={i} style={rowStyle} className={headings[i]}>{value}</td>;
        }
      )}
      </tr>);
  }
});