/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');
const CompLibrary = require('../../core/CompLibrary.js');

const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

function imgUrl(img) {
  return siteConfig.baseUrl + 'img/' + img;
}

class HomeSplash extends React.Component {
  render() {
    const {siteConfig, language = ''} = this.props;
    const {baseUrl, docsUrl} = siteConfig;
    const docsPart = `${docsUrl ? `${docsUrl}/` : ''}`;
    const langPart = `${language ? `${language}/` : ''}`;
    const docUrl = doc => `${baseUrl}${docsPart}${langPart}${doc}`;

    const SplashContainer = props => (
      <div className="homeContainer">
        <div className="homeSplashFade">
          <div className="wrapper homeWrapper">{props.children}</div>
        </div>
      </div>
    );

    const Logo = props => (
      <div className="" style={{width: '100%', alignItems: 'center', margin: 'auto'}}>
        <img src={props.img_src} />
      </div>
    );

    const ProjectTitle = () => (
      <h2 className="projectTitle">
        {siteConfig.title}
        <small>{siteConfig.tagline}</small>
      </h2>
    );

    const PromoSection = props => (
      <div className="section promoSection">
        <div className="promoRow">
          <div className="pluginRowBlock">{props.children}</div>
        </div>
      </div>
    );

    const Button = props => (
      <div className="pluginWrapper buttonWrapper">
        <a className="button" href={props.href} target={props.target}>
          {props.children}
        </a>
      </div>
    );

    return (
      <SplashContainer>
        <Logo img_src={`${baseUrl}img/HeronTextLogo.png`} />
        <div className="inner">
          <ProjectTitle siteConfig={siteConfig} />
          <PromoSection>
      < Button
      href = "/docs/getting-started-local-single-node" > Documentation < /Button>
            {/* keep as reference for now <Button href={docUrl('doc1.html')}>Github</Button> */}
            <Button href="https://github.com/apache/incubator-heron" target="_blank">Github</Button>
          </PromoSection>
        </div>
      </SplashContainer>
    );
  }
}

class Index extends React.Component {
  render() {
    const {config: siteConfig, language = ''} = this.props;
    const {baseUrl} = siteConfig;

    const Block = props => (
      <Container
        padding={['bottom', 'top']}
        id={props.id}
        background={props.background}>
        <GridBlock
          align="center"
          contents={props.children}
          layout={props.layout}
        />
      </Container>
    );
   

    const Features = () => (
      <Block layout="threeColumn">
        {[
          {
            content: 'Heron is built with a wide array of architectural improvements that contribute to high efficiency gains.',
            imageAlign: 'top',
            title: 'Speed and Performance',
          },
          {
            content: 'Heron has powered all realtime analytics with varied use cases at Twitter since 2014. Incident reports dropped by an order of magnitude demonstrating proven reliability and scalability',
            imageAlign: 'top',
            title: 'Proven at Twitter Scale',
          },
          {
            content: 'Heron is API compatible with Apache Storm and hence no code change is required for migration.',
            imageAlign: 'top',
            title: 'Compatibility with Storm',
          },
          {
            content: 'Easily debug and identify the issues in topologies, allowing faster iteration during development.',
            imageAlign: 'top',
            title: 'Ease of Development and Troubleshooting',
          },
          {
            content: 'Heron UI gives a visual overview of each topology to visualize hot spot locations and detailed counters for tracking progress and troubleshooting.',
            imageAlign: 'top',
            title: 'Simplified and Responsive UI',
          },
          {
            content: 'Heron is highly scalable both in the ability to execute large number of components for each topology and the ability to launch and track large numbers of topologies.',
            imageAlign: 'top',
            title: 'Scalability and Reliability',
          },
        ]}
      </Block>
    );

    

    return (
      <div>
        <HomeSplash siteConfig={siteConfig} language={language} />
        <div className="mainContainer">
          <Features />
        </div>
      </div>
    );
  }
}

module.exports = Index;
