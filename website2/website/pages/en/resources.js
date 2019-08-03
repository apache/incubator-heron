

const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');
const Container = CompLibrary.Container;

const CWD = process.cwd();

const translate = require('../../server/translate.js').translate;

const siteConfig = require(`${CWD}/siteConfig.js`);
const resources = require(`${CWD}/data/resources.js`)

class Resources extends React.Component {
  render() {
    let language = this.props.language || '';
    

    return (
      <div className="docMainWrapper wrapper">
        <Container className="mainContainer documentContainer postContainer">
          <div className="post">
            <header className="postHeader">
              <h1><translate>Resources</translate></h1>
              <hr />
            </header>
            
            <h2><translate>Articles</translate></h2>
            <table className="versions">
              <thead>
                <tr>
                  <th><translate>Link</translate></th>
                </tr>
              </thead>
              <tbody>
                {resources.publications.map(
                  (a, i) => (
                    <tr key={i}>
                      <td><a href={a.link}>{a.title}</a></td>
                    </tr>
                  )
                )}
              </tbody>
            </table>

            <h2><translate>Presentations</translate></h2>
            <table className="versions">
              <thead>
                <tr>
                  <th><translate>Forum</translate></th>
                  <th><translate>Data</translate></th>
                  <th><translate>Presenter</translate></th>
                  <th><translate>Link</translate></th>
                </tr>
              </thead>
              <tbody>
                {resources.presentations.map(
                  (p, i) => (
                    <tr key={i}>
                      <td><a href={p.forum_link}>{p.forum}</a></td>
                      <td>{p.date}</td>
                      <td>{p.presenter}</td>
                      <td><a href={p.link}>{p.title}</a></td>
                    </tr>
                  )
                )}
              </tbody>
            </table>
            <h2><translate>Blogs</translate></h2>
            <table className="versions">
              <thead>
                <tr>
                  <th><translate>Forum</translate></th>
                  <th><translate>Data</translate></th>
                  <th><translate>Presenter</translate></th>
                  <th><translate>Link</translate></th>
                </tr>
              </thead>
              <tbody>
                {resources.blogs.map(
                  (p, i) => (
                    <tr key={i}>
                      <td><a href={p.forum_link}>{p.forum}</a></td>
                      <td>{p.date}</td>
                      <td>{p.presenter}</td>
                      <td><a href={p.link}>{p.title}</a></td>
                    </tr>
                  )
                )}
              </tbody>
            </table>
            <h2><translate>Press</translate></h2>
            <table className="versions">
              <thead>
                <tr>
                  <th><translate>Data</translate></th>
                  <th><translate>Presenter</translate></th>
                  <th><translate>Link</translate></th>
                </tr>
              </thead>
              <tbody>
                {resources.press.map(
                  (p, i) => (
                    <tr key={i}>
                      <td>{p.date}</td>
                      <td>{p.presenter}</td>
                      <td><a href={p.link}>{p.title}</a></td>
                    </tr>
                  )
                )}
              </tbody>
            </table>

          </div>
        </Container>
      </div>
    );
  }
}

module.exports = Resources;