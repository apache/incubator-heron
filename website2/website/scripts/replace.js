const replace = require('replace-in-file');

const fs = require('fs')

const CWD = process.cwd()
const siteConfig = require(`${CWD}/siteConfig.js`);
const docsDir = `${CWD}/build/${siteConfig.projectName}/docs`

function getVersions() {
    try {
      return JSON.parse(require('fs').readFileSync(`${CWD}/versions.json`, 'utf8'));
    } catch (error) {
      //console.error(error)
      console.error('no versions found defaulting to 2.1.0')
    }
    return ['2.1.0']
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