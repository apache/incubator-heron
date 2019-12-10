/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');

class Footer extends React.Component {
  docUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl;
    const docsUrl = this.props.config.docsUrl;
    const docsPart = `${docsUrl ? `${docsUrl}/` : ''}`;
    const langPart = `${language ? `${language}/` : ''}`;
    return `${baseUrl}${docsPart}${langPart}${doc}`;
  }

  pageUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl;
    return baseUrl + (language ? `${language}/` : '') + doc;
  }

  render() {
    return (
      <footer className="nav-footer" id="footer">

        <div className="apache-disclaimer">
          Apache Heron is an effort undergoing incubation at <a target="_blank" href="https://apache.org/">The Apache Software Foundation (ASF)</a> sponsored by the Apache Incubator PMC.
          Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects.
          While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.
          <br></br>
          <br></br>
          Apache®, the names of Apache projects, and the feather logo are either <a rel="external" href="https://www.apache.org/foundation/marks/list/">registered trademarks or trademarks</a> of the Apache Software Foundation in the United States and/or other countries.
          <br></br>
          <br></br>
          <div className="copyright-box">{this.props.config.copyright}</div>

        </div>
        <div className="apache-links">
        <a class="item" rel="external" href="https://incubator.apache.org/">Apache Incubator</a>
            <div><a class="item" rel="external" href="https://www.apache.org/">About the ASF</a></div>
            <div><a class="item" rel="external" href="https://www.apache.org/events/current-event">Events</a></div>
            <div><a class="item" rel="external" href="https://www.apache.org/foundation/thanks.html">Thanks</a></div>
            <div><a class="item" rel="external" href="https://www.apache.org/foundation/sponsorship.html">Become a Sponsor</a></div>
            <div><a class="item" rel="external" href="https://www.apache.org/security/">Security</a></div>
            <div><a class="item" rel="external" href="https://www.apache.org/licenses/">License</a></div>
        </div>

      </footer>
    );
  }
}

module.exports = Footer;
