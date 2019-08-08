const replace = require('replace-in-file');

const fs = require('fs')

const CWD = process.cwd()
const siteConfig = require(`${CWD}/siteConfig.js`);
const docsDir = `${CWD}/build/${siteConfig.projectName}/docs`

function getVersions() {
    try {
      console.log(JSON.parse(require('fs').readFileSync(`${CWD}/versions.json`, 'utf8')))
      return JSON.parse(require('fs').readFileSync(`${CWD}/versions.json`, 'utf8'));
    } catch (error) {
      //console.error(error)
      console.error('no versions found defaulting to 0.20.0')
    }
    return ['0.20.0']
  }

function doReplace(options) {
replace(options)
    .then(changes => {
    if (options.dry) {
        console.log('Modified files:');
        console.log(changes.join('\n'))
    }
    })
    .catch(error => {
    console.error('Error occurred:', error);
    });
}

const versions = getVersions();

const latestVersion = versions[0];

console.log(latestVersion)
const from = [
    /{{heron:version_latest}}/g,
    /{{heron:version}}/g,
];

const options = {
    files: [
      `${docsDir}/*.html`,
      `${docsDir}/**/*.html`
    ],
    ignore: versions.map(v => `${docsDir}/${v}/**/*`), // TODO add next and assets
    from: from,
    to: [
      `${latestVersion}`,
      `${versions}`,
    ],
    dry: false
  };
  
doReplace(options);

// replaces versions
for (v of versions) {
    if (v === latestVersion) {
      continue
    }
    const opts = {
      files: [
        `${docsDir}/${v}/*.html`,
        `${docsDir}/${v}/**/*.html`
      ],
      from: from,
      to: [
        `${latestVersion}`,
        `${v}`,
      ],
      dry: true
    };
    doReplace(opts);
}  