const React = require('react');

const CompLibrary = require('../../core/CompLibrary');
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();

const siteConfig = require(`${CWD}/siteConfig.js`);
const heronReleases = require(`${CWD}/heron-release.json`)

function getLatestArchiveMirrorUrl(version, type) {
    return `http://www.apache.org/dyn/closer.cgi/incubator/heron/heron-${version}/heron-${version}-${type}.tar.gz?action=download`
}

function getTarUrl(version, type) {
   return `https://downloads.apache.org/incubator/heron/heron-${version}/heron-${version}-${type}.tar.gz`
}

function getInstallScriptCryptoUrl(version, osType) {
   return `https://downloads.apache.org/incubator/heron/heron-${version}/heron-install-${version}-${osType}.sh`
}

function distUrl(version, type) {
    return `http://www.apache.org/dyn/closer.cgi/incubator/heron/heron-${version}/heron-${version}-${type}.tar.gz?action=download`
}

function getInstallScriptMirrorUrl(version, type) {
    return `http://www.apache.org/dyn/closer.cgi/incubator/heron/heron-${version}/heron-install-${version}-${type}.sh`
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
    const latestArchiveMirrorUrl = getLatestArchiveMirrorUrl(latestHeronVersion, 'bin');
    const latestSrcArchiveMirrorUrl = getLatestArchiveMirrorUrl(latestHeronVersion, 'src');
    const latestSrcUrl = getTarUrl(latestHeronVersion, "src");
    const latestDebian10TarUrl =  getTarUrl(latestHeronVersion, "debian10");
    const latestArchiveUrl = distUrl(latestHeronVersion, 'bin');
    const latestSrcArchiveUrl = distUrl(latestHeronVersion, 'src')
    const centos7InstallUrl = getInstallScriptMirrorUrl(latestHeronVersion, "centos7")
    const centos7InstallCryptoUrl = getInstallScriptCryptoUrl(latestHeronVersion, "centos7")
    const debian10InstallUrl = getInstallScriptMirrorUrl(latestHeronVersion, "debian10")
    const debian10InstallCryptoUrl = getInstallScriptCryptoUrl(latestHeronVersion, "debian10")
    const ubuntu1804InstallUrl = getInstallScriptMirrorUrl(latestHeronVersion, "ubuntu18.04")
    const ubuntu1804InstallCryptoUrl = getInstallScriptCryptoUrl(latestHeronVersion, "ubuntu18.04")



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
                <tr key={'binary'}>
                  <th>Debian10 Binary</th>
                  <td>
                    <a href={latestSrcArchiveMirrorUrl}>heron-{latestHeronVersion}-debian10.tar.gz</a>
                  </td>
                  <td>
                    <a href={`${latestDebian10TarUrl}.asc`}>asc</a>,&nbsp;
                    <a href={`${latestDebian10TarUrl}.sha512`}>sha512</a>
                  </td>
                </tr>
                </tbody>
              </table>

              <h2 id="latest">Heron Install Scripts</h2>
              <h3 style={{color:"red"}}> READ BEFORE DOWNLOADING </h3>
              <p>
                To download the Heron self-extracting install scripts: click a link below for the Operating System of your choice that will show the closest mirror for you to download from.
                 Once you are on the page with the closest mirror right click on the link and select “save as” to download the install script.
                  If you do not right click the link will only bring you to view the script in the browser and will not start a download.
              </p>
              <table className="versions" style={{width:'100%'}}>
                <thead>
                  <tr>
                    <th>Release</th>
                    <th>Link to find the closest mirror</th>
                    <th>Crypto files</th>
                  </tr>
                </thead>
                <tbody>


                  <tr key={'centos-install'}>
                    <th>CentOS7</th>
                    <td>
                      <a href={`${centos7InstallUrl}`}> heron-install-0.20.4-incubating-centos7.sh</a>
                    </td>
                    <td>
                      <a href={`${centos7InstallCryptoUrl}.asc`}>asc</a>,&nbsp;
                      <a href={`${centos7InstallCryptoUrl}.sha512`}>sha512</a>
                    </td>
                  </tr>
                  <tr key={'debian10-install'}>
                    <th>Debian10</th>
                    <td>
                      <a href={`${debian10InstallUrl}`}> heron-install-0.20.4-incubating-debian10.sh</a>
                    </td>
                    <td>
                      <a href={`${debian10InstallCryptoUrl}.asc`}>asc</a>,&nbsp;
                      <a href={`${debian10InstallCryptoUrl}.sha512`}>sha512</a>
                    </td>
                  </tr>
                   <tr key={'ubuntu18.04-install'}>
                    <th>Ubuntu18.04</th>
                    <td>
                     <a href={`${ubuntu1804InstallUrl}`}> heron-install-0.20.4-incubating-ubuntu18.04.sh</a>
                    </td>
                    <td>
                      <a href={`${ubuntu1804InstallCryptoUrl}.asc`}>asc</a>,&nbsp;
                      <a href={`${ubuntu1804InstallCryptoUrl}.sha512`}>sha512</a>
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

                        return info.version !== latestHeronVersion && (
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