
const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');
const Container = CompLibrary.Container;
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const GridBlock = CompLibrary.GridBlock;
const translate = require('../../server/translate.js').translate;

const CWD = process.cwd();

const siteConfig = require(`${CWD}/siteConfig.js`);

class Contact extends React.Component {
  render() {
    let language = this.props.language || '';
    const mailingLists = [
      {
        email: 'users@heron.incubator.apache.org',
        desc: 'User-related discussions',
        subscribe: 'mailto:user-subscribe@heron.incubator.apache.org',
        unsubscribe: 'mailto:users-unsubscribe@heron.incubator.apache.org',
        archives: 'http://mail-archives.apache.org/mod_mbox/heron-user/'
      },
      {
        email: 'dev@heron.incubator.apache.org',
        desc: 'Development-related discussions',
        subscribe: 'mailto:dev-subscribe@heron.incubator.apache.org',
        unsubscribe: 'mailto:dev-unsubscribe@heron.incubator.apache.org',
        archives: 'http://mail-archives.apache.org/mod_mbox/heron-dev/'
      },
      {
        email: 'commits@heron.incubator.apache.org',
        desc: 'All commits to the Heron repository',
        subscribe: 'mailto:commits-subscribe@heron.incubator.apache.org',
        unsubscribe: 'mailto:commits-unsubscribe@heron.incubator.apache.org',
        archives: 'http://mail-archives.apache.org/mod_mbox/heron-commits/'
      }
    ]

    return (
      <div className="docMainWrapper wrapper">
        <Container className="mainContainer documentContainer postContainer">
          <div className="post">
            <header className="postHeader">
              <h1><translate>Contact</translate></h1>
              <hr />
            </header>
            <p><translate>
            There are many ways to get help from the Apache Heron community.
            The mailing lists are the primary place where all Heron committers are present.
            Bugs and feature requests can either be discussed on the dev mailing list or
            by opening an issue on
            <a href="https://github.com/apache/incubator-heron/" target="_blank">GitHub</a>.
            </translate></p>

            <h2><translate>Mailing Lists</translate></h2>
            <table className="versions">
              <thead>
                <tr>
                  <th><translate>Name</translate></th>
                  <th><translate>Scope</translate></th>
                  <th></th>
                  <th></th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {mailingLists.map(
                  list => (
                      <tr key={list.email}>
                        <td>{list.email}</td>
                        <td>{list.desc}</td>
                        <td><a href={list.subscribe}><translate>Subscribe</translate></a></td>
                        <td><a href={list.unsubscribe}><translate>Unsubscribe</translate></a></td>
                        <td><a href={list.archives}><translate>Archives</translate></a></td>
                      </tr>
                    )
                )}
              </tbody>
            </table>

            <h2><translate>Slack</translate></h2>
            <p><translate>There is a Heron slack channel that is used for informal discussions for devs and users.</translate></p>

            <p><translate>The Slack instance is at </translate> <a href="https://heronstreaming.slack.com/" target="_blank">
                    https://heronstreaming.slack.com/</a></p>

            <p><translate>You can self-register at </translate> <a href="http://heronstreaming.herokuapp.com/" target="_blank">
                    http://heronstreaming.herokuapp.com/</a></p>
          </div>
        </Container>
      </div>
    );
  }
}

module.exports = Contact;