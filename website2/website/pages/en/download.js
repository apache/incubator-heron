const React = require('react');

const CompLibrary = require('../../core/CompLibrary');
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();

const siteConfig = require(`${CWD}/siteConfig.js`);
const heronReleases = require(`${CWD}/heron-release.json`)

function getLatestArchiveMirrorUrl(version, type) {
    return `http://www.apache.org/dyn/closer.lua/incubator/heron/heron-${version}/heron-${version}-${type}.tar.gz?action=download`
}

function getTarUrl(version, type) {
   return `https://downloads.apache.org/incubator/heron/heron-${version}/heron-${version}-${type}.tar.gz`
}

function getInstallScriptCryptoUrl(version, osType) {
   return `https://downloads.apache.org/incubator/heron/heron-${version}/heron-install-${version}-${osType}.sh.tar.gz`
}

function distUrl(version, type) {
    return `http://www.apache.org/dyn/closer.lua/incubator/heron/heron-${version}/heron-${version}-${type}.tar.gz?action=download`
}

function getInstallScriptMirrorUrl(version, type) {
    return `http://www.apache.org/dyn/closer.lua/incubator/heron/heron-${version}/heron-install-${version}-${type}.sh.tar.gz`
}

function archiveUrl(version, type) {
    if (version.includes('incubating')) {

        return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=/incubator/heron/heron-${version}/` + getProperEndpoint(version, type)
    } else {
        return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=heron/heron-${version}/` + getProperEndpoint(version, type)
    }
}

function getProperEndpoint(version, type) {

  if (version == "0.20.0-incubating") {
   return `apache-heron-v-${version}-${type}.tar.gz`
  }
  if (type == "source") {
    type = "src";
  }
  return `heron-${version}-${type}.tar.gz`
}



class Download extends React.Component {
  render() {
    const latestHeronVersion = heronReleases[0];
    const latestSrcArchiveMirrorUrl = getLatestArchiveMirrorUrl(latestHeronVersion, 'src');
    const latestSrcUrl = getTarUrl(latestHeronVersion, "src");
    const centOS7InstallUrl = getInstallScriptMirrorUrl(latestHeronVersion, "centos7")
    const centOS7InstallCryptoUrl = getInstallScriptCryptoUrl(latestHeronVersion, "centos7")
    const darwinInstallUrl = getInstallScriptMirrorUrl(latestHeronVersion, "darwin")
    const darwinInstallCryptoUrl = getInstallScriptCryptoUrl(latestHeronVersion, "darwin")
    const debian11InstallUrl = getInstallScriptMirrorUrl(latestHeronVersion, "debian11")
    const debian11InstallCryptoUrl = getInstallScriptCryptoUrl(latestHeronVersion, "debian11")
    const ubuntu2204InstallUrl = getInstallScriptMirrorUrl(latestHeronVersion, "ubuntu22.04")
    const ubuntu2204InstallCryptoUrl = getInstallScriptCryptoUrl(latestHeronVersion, "ubuntu22.04")



    const releaseInfo = heronReleases.map(version => {
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
                  <a href="https://heron.apache.org/release-notes">Release notes</a> for all of Heron's versions
                </p>
              </div>

            <h2 id="latest">Current version (Stable) {latestHeronVersion}</h2>
            <table className="versions" style={{width:'100%'}}>
              <thead>
                <tr>
                  <th>Release</th>
                  <th>Direct Download Link</th>
                  <th>Crypto files</th>
                </tr>
              </thead>
              <tbody>


                <tr key={'source'}>
                  <th>Source</th>
                  <td>
                    <a href={latestSrcArchiveMirrorUrl}>heron-{latestHeronVersion}-src.tar.gz</a>
                  </td>
                  <td>
                    <a href={`${latestSrcUrl}.asc`}>asc</a>,&nbsp;
                    <a href={`${latestSrcUrl}.sha512`}>sha512</a>
                  </td>
                </tr>

                </tbody>
              </table>

              <h2 id="latest">Heron Install Scripts</h2>

              <table className="versions" style={{width:'100%'}}>
                <thead>
                  <tr>
                    <th>Release</th>
                    <th>Link to find the closest mirror</th>
                    <th>Crypto files</th>
                  </tr>
                </thead>
                <tbody>


                  <tr key={'centos7-install'}>
                    <th>CentOS 7</th>
                    <td>
                      <a href={`${centOS7InstallUrl}`}> heron-install-{latestHeronVersion}-centos7.sh.tar.gz</a>
                    </td>
                    <td>
                      <a href={`${centOS7InstallCryptoUrl}.asc`}>asc</a>,&nbsp;
                      <a href={`${centOS7InstallCryptoUrl}.sha512`}>sha512</a>
                    </td>
                  </tr>
                  <tr key={'darwin-install'}>
                    <th>Darwin</th>
                    <td>
                      <a href={`${darwinInstallUrl}`}> heron-install-{latestHeronVersion}-darwin.sh.tar.gz</a>
                    </td>
                    <td>
                      <a href={`${darwinInstallCryptoUrl}.asc`}>asc</a>,&nbsp;
                      <a href={`${darwinInstallCryptoUrl}.sha512`}>sha512</a>
                    </td>
                  </tr>
                  <tr key={'debian11-install'}>
                    <th>debian11</th>
                    <td>
                      <a href={`${debian11InstallUrl}`}> heron-install-{latestHeronVersion}-debian11.sh</a>
                    </td>
                    <td>
                      <a href={`${debian11InstallCryptoUrl}.asc`}>asc</a>,&nbsp;
                      <a href={`${debian11InstallCryptoUrl}.sha512`}>sha512</a>
                    </td>
                  </tr>
                   <tr key={'ubuntu22.04-install'}>
                    <th>Ubuntu22.04</th>
                    <td>
                     <a href={`${ubuntu2204InstallUrl}`}> heron-install-{latestHeronVersion}-ubuntu22.04.sh</a>
                    </td>
                    <td>
                      <a href={`${ubuntu2204InstallCryptoUrl}.asc`}>asc</a>,&nbsp;
                      <a href={`${ubuntu2204InstallCryptoUrl}.sha512`}>sha512</a>
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
            <p>Older releases can be found at <a href="https://archive.apache.org/dist/incubator/heron/"> the archive page.</a></p>
          </div>
        </Container>
      </div>
    );
  }
}

module.exports = Download;
