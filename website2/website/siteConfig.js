/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// See https://docusaurus.io/docs/site-config for all the possible
// site configuration options.

// To support embed [variable injection's..]
// https://www.npmjs.com/package/remarkable-embed
const {Plugin: Embed} = require('remarkable-embed');

// Our custom remarkable plugin factory.
const createVariableInjectionPlugin = variables => {
  // `let` binding used to initialize the `Embed` plugin only once for efficiency.
  // See `if` statement below.
  let initializedPlugin;

  const embed = new Embed();
  embed.register({
    // Call the render method to process the corresponding variable with
    // the passed Remarkable instance.
    // -> the Markdown markup in the variable will be converted to HTML.
    inject: (key) => {
      if (keyparts[0] == 'javadoc') {
        return renderUrl(initializedPlugin, javadocUrl, keyparts);
    // githubUrl:<name>:<path>
      }
    else {
      keyparts = key.split("|");
      // endpoint api: endpoint|<op>
      if (keyparts[0] == 'endpoint') {
          return renderEndpoint(initializedPlugin, restApiUrl + "#", keyparts);
      }
    }

      return initializedPlugin.render(variables[key])
    }
    // inject: key => initializedPlugin.render(variables[key])
  });

  return (md, options) => {
    if (!initializedPlugin) {
      initializedPlugin = {
        render: md.render.bind(md),
        hook: embed.hook(md, options)
      };
    }

    return initializedPlugin.hook;
  };
};

const url = 'https://heron.incubator.apache.org';
const baseUrl = '/';

const siteVariables = {
};

const siteConfig = {
  title: 'Apache Heron', // Title for your website.
  tagline: 'A realtime, distributed, fault-tolerant stream processing engine',
  // For github.io type URLs, you would set the url and baseUrl like:
  url: url,
  baseUrl: baseUrl, // Base URL for your project */

  // Used for publishing and more
  projectName: 'incubator-heron',
  organizationName: 'Apache',

  // For no header links in the top nav bar -> headerLinks: [],
  headerLinks: [
    {href: '/api/java', label: "Javadocs"},
    {href: '/api/python', label: "Pydocs"},
    {doc: 'getting-started-local-single-node', label: 'Docs'},
    {href: '#community', label: 'Community'},
    //{blog: true, label: 'Blog'},
    {href: '#apache', label: 'Apache'},
    // {page: 'download', label: 'Download'},
    // Drop down for languages
    // { languages: true }
  ],
  // explicitly set the flag to allow for indexing of the site.
  noIndex: 'false',

  /* path to images for header/footer */
  headerIcon: 'img/HeronTextLogo-small.png',
  footerIcon: '',
  favicon: 'img/favicon-32x32.png',

  /* Colors for website */
  colors: {
    primaryColor: '#263238',
    secondaryColor: '#1d3f5f',
  },


  // This copyright info is used in /core/Footer.js and blog RSS/Atom feeds.
  copyright: `Copyright © ${new Date().getFullYear()} the Apache Software Foundation, Apache Heron, Heron, 
  Apache, the Apache feather Logo, and the Apache Heron project logo are either registered 
  trademarks or trademarks of the Apache Software Foundation.`,

  highlight: {
    // Highlight.js theme to use for syntax highlighting in code blocks.
    theme: 'default',
  },

  // Add custom scripts here that would be placed in <script> tags.
  scripts: [
    'https://buttons.github.io/buttons.js',
    `${baseUrl}js/custom.js`
  ],

  // On page navigation for the current documentation page.
  onPageNav: 'separate',
  // No .html extensions for paths.
  cleanUrl: true,
  scrollToTopOptions: {
    zIndex: 100,
  },

  // Open Graph and Twitter card images.
  twitter: true,
  twitterUsername: 'apache_pulsar',
  ogImage: 'img/undraw_online.svg',
  twitterImage: 'img/undraw_tweetstorm.svg',

  markdownPlugins: [
    createVariableInjectionPlugin(siteVariables)
  ],
};

module.exports = siteConfig;
