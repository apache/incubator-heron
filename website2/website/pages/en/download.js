const React = require('react');

const CompLibrary = require('../../core/CompLibrary');
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();

const siteConfig = require(`${CWD}/siteConfig.js`);
const releases = require(`${CWD}/releases.json`);
const heronReleases = require(`${CWD}/heron-release.json`)

function getLatestArchiveMirrorUrl(version, type) {
    return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=incubator/heron/heron-${version}/heron-${version}-${type}.tar.gz`
}

function distUrl(version, type) {
    return `https://www.apache.org/dist/incubator/heron/heron-${version}/heron-${version}-${type}.tar.gz`
}

function archiveUrl(version, type) {
    if (version.includes('incubating')) {
        return `https://archive.apache.org/dist/incubator/heron/heron-${version}/apache-heron-v-${version}-${type}.tar.gz`
    } else {
        return `https://archive.apache.org/dist/heron/heron-${version}/apache-heron-v-${version}-${type}.tar.gz`
    }
}



class Download extends React.Component {
  render() {
    const latestVersion = releases[0];
    const latestHeronVersion = heronReleases[0];
    const latestArchiveMirrorUrl = getLatestArchiveMirrorUrl(latestVersion, 'bin');
    const latestSrcArchiveMirrorUrl = getLatestArchiveMirrorUrl(latestVersion, 'src');
    const latestArchiveUrl = distUrl(latestVersion, 'bin');
    const latestSrcArchiveUrl = distUrl(latestVersion, 'src')

    const releaseInfo = releases.map(version => {
      return {
        version: version,
        binArchiveUrl: archiveUrl(version, 'bin'),
        srcArchiveUrl: archiveUrl(version, 'source')
      }
    });


    return (
      <div className="pageContainer">
        <Container className="mainContainer documentContainer postContainer">
          <div className="post">
            <header className="postHeader">
              <h1>Apache Heron (Incubating) downloads</h1>
              <hr />
            </header>

            <h2>Release notes</h2>
            <div>
              <p>
                <a href={`${siteConfig.baseUrl}${this.props.language}/release-notes`}>Release notes</a> for all Heron's versions
              </p>
            </div>

            <h2 id="latest">Current version (Stable) {latestHeronVersion}</h2>
            <table className="versions" style={{width:'100%'}}>
              <thead>
                <tr>
                  <th>Release</th>
                  <th>Link</th>
                  <th>Crypto files</th>
                </tr>
              </thead>
              <tbody>


                <tr key={'source'}>
                  <th>Source</th>
                  <td>
                    <a href={latestSrcArchiveMirrorUrl}>apache-heron-{latestVersion}-src.tar.gz</a>
                  </td>
                  <td>
                    <a href={`${latestSrcArchiveUrl}.asc`}>asc</a>,&nbsp;
                    <a href={`${latestSrcArchiveUrl}.sha512`}>sha512</a>
                  </td>
                </tr>
                </tbody>
              </table>


            <h2>Release Integrity</h2>
            <MarkdownBlock>
              You must [verify](https://www.apache.org/info/verification.html) the integrity of the downloaded files.
              We provide OpenPGP signatures for every release file. This signature should be matched against the
              [KEYS](https://downloads.apache.org/incubator/heron/KEYS) file which contains the OpenPGP keys of
              Herons's Release Managers. We also provide `SHA-512` checksums for every release file.
              After you download the file, you should calculate a checksum for your download, and make sure it is
              the same as ours.
            </MarkdownBlock>

            <h2>Getting started</h2>
            <div>
              <p>

                Once you've downloaded a Heron release, instructions on getting up and running with a standalone cluster
                that you can run on your laptop can be found in the{' '}
              &nbsp;
                <a href={`${siteConfig.baseUrl}docs/getting-started-local-single-node`}>run Heron locally</a> tutorial.
              </p>
            </div>


            <h2 id="archive">Older releases</h2>
            <table className="versions">
              <thead>
                <tr>
                  <th>Release</th>

                  <th>Source</th>
                  <th>Release notes</th>
                </tr>
              </thead>
              <tbody>
                {releaseInfo.map(
                  info => {
                        var sha = "sha512"
                        if (info.version.includes('1.19.0-incubating') || info.version.includes('1.20.0-incubating')) {
                            sha = "sha"
                        }
                        return info.version !== latestVersion && (
                            <tr key={info.version}>
                        <th>{info.version}</th>

                          <td>
                          <a href={info.srcArchiveUrl}>apache-heron-{info.version}-source.tar.gz</a>
                              &nbsp;
                          (<a href={`${info.srcArchiveUrl}.asc`}>asc</a>,&nbsp;
                          <a href={`${info.srcArchiveUrl}.${sha}`}>{`${sha}`}</a>)
                          </td>
                          <td>
                          <a href={`${siteConfig.baseUrl}${this.props.language}/release-notes#${info.version}`}>Release Notes</a>
                          </td>
                          </tr>
                      )
                    }
                )}
              </tbody>
            </table>
          </div>
        </Container>
      </div>
    );
  }
}

module.exports = Download;