
const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');
const Container = CompLibrary.Container;
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();

const translate = require('../../server/translate.js').translate;

const siteConfig = require(`${CWD}/siteConfig.js`);

class Events extends React.Component {
  render() {

    return (
      <div className="docMainWrapper wrapper">
        <Container className="mainContainer documentContainer postContainer">
          <div className="post">
            <header className="postHeader">
              <h1><translate>Events</translate></h1>
              <hr />
            </header>
            
            <h2><translate>Groups</translate></h2>
            <MarkdownBlock>
              - [Apache Heron Bay Area Meetup Group](https://www.meetup.com/Apache-Heron-Bay-Area/events/249414421/)
            </MarkdownBlock>
          </div>
        </Container>
      </div>
    );
  }
}

module.exports = Events;