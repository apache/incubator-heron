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

const bazelVersions = {
    '0.20.0-incubating': '0.14.1',
    '0.20.1-incubating': '0.26.0',
    '0.20.2-incubating': '0.26.0',
    'latest': '0.26.0',
}

function replaceBazel(version) {
    try {
        if (version in bazelVersions) {
            return bazelVersions[version]
        } else {
            throw new Error('Unable to find bazel Version');
        }
    } catch (error) {

        console.error('no versions found defaulting to 0.26')
    }
    return '0.26'
}

console.log(latestVersion)
const from = [
    /{{heron:version}}/g,
    /{{bazel:version}}/g,
    /{{% heronVersion %}}/g,
    /{{% bazelVersion %}}/g
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
        replaceBazel(`${latestVersion}`),
        `${latestVersion}`,
        replaceBazel(`${latestVersion}`),
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
          `${v}`,
          replaceBazel(`${v}`),
          `${v}`,
          replaceBazel(`${v}`),
      ],
        dry: false
    };
    doReplace(opts);
}  