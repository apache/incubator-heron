<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# The Heron website and documentation

This `README` is basically the meta-documentation for the Heron website and documentation. You will find instructions on running the site locally.

## Tools

Framework [Docusaurus](https://docusaurus.io/).

Ensure you have installed the latest version of [Node](https://nodejs.org/en/download/). You can install [Yarn](https://yarnpkg.com/en/docs/install) as well.

> You have to be on Node >= 8.x and Yarn >= 1.5.


## Running the site locally

To run the site locally:

```bash 
git clone https://github.com/apache/incubator-heron.git
cd incubator-heron/website2/website
yarn install
yarn start
```
> Notes
> 
> 1. If you have installed `yarn`, you can skip the `yarn install` command.
> 2. After you enter the `yarn start` command, you will be navigated to a local address, for example, `http://localhost:3000`. Click `Docs` to see documentation for the latest release of Heron. 
> 3. The `http://localhost:3000/en/versions` path shows the documentation for all versions. To view your local changes, click `Documentation` in **Latest Version**, or enter `http://localhost:3000/docs/en/next/standalone` in a browser.

## Contribute

The website is comprised of two parts: one is documentation, the other is website pages (including blog posts).

Documentation related pages are placed in the `docs` directory. They are written in [Markdown](http://daringfireball.net/projects/markdown/syntax).
All documentation pages are versioned. For more details, refer to [versioning](#versioning).

Website pages are non-versioned. They are placed in the `website` directory.

### Documentation

#### Layout

All the markdown files are placed in the `docs` directory. It is a flat structure.

```
docs
|
├── cluster-config-instance.md
├── cluster-config-metrics.md
├── cluster-config-overview.md
├── cluster-config-stream.md
├── cluster-config-system-level.md
├── cluster-config-tmanager.md
├── compiling-code-organization.md
├── compiling-docker.md
├── compiling-linux.md
├── compiling-osx.md
├── compiling-overview.md
├── compiling-running-tests.md
├── deployment-api-server.md
...
```

All the files are named in the following convention:

```
<category>-<page-name>.md
```

`<category>` is the category within the sidebar that this file belongs to, while `<page-name>` is the string to name the file within this category.

There isn't any constraints on how files are named. It is just a naming convention for better maintenance.

#### Document

##### Markdown Headers

All the documents are usual Markdown files. However you need to add some Docusaurus-specific fields in Markdown headers in order to link them
correctly to the [Sidebar](#sidebar) and [Navigation Bar](#navigation).

`id`: A unique document ID. If this field is not specified, the document ID defaults to its file name (without the extension).

`title`: The title of the document. If this field is not specified, the document title defaults to its id.

`hide_title`: Whether to hide the title at the top of the doc.

`sidebar_label`: The text shown in the document sidebar for this document. If this field is not specified, the document `sidebar_label` defaults to its title.

For example:

```bash
---
id: extending-heron-scheduler
title: Implementing a Custom Scheduler
sidebar_label: Custom Scheduler
---
```

##### Linking to another document

To link to other documentation files, you can use relative URLs, which will be automatically converted to the corresponding HTML links when they are rendered.

Example:

```md
[This links to another document](other-document.md)
```

The markdown file will be automatically converted into a link to /docs/other-document.html (or the appropriately translated/versioned link) once it is rendered.

This helps when you want to navigate through docs on GitHub since the links there are functional links to other documents (still on GitHub),
and the documents have the correct HTML links when they are rendered.

#### Sidebar

All the sidebars are defined in a `sidebars.json` file in the `website` directory. The documentation sidebar is named `docs` in the JSON structure.

When you want to add a page to sidebar, you can add the document `id` you used in the document header to the existing sidebar/category. In the example below,
`docs` is the name of the sidebar, "Getting started" is a category within the sidebar, and "getting-started-local-single-node" is the `id` of a document.

```bash
{
  "docs": {
    "Getting Started": [
      "getting-started-local-single-node",
      "getting-started-migrate-storm-topologies",
      "getting-started-troubleshooting-guide"
    ],
    ...
  }
}
```

#### Navigation

To add links to the top navigation bar, you can add entries to the `headerLinks` of `siteConfig.js` under `website` directory.

To learn different types of links you can add to the top navigation bar, refer to [Navigation and Sidebars](https://docusaurus.io/docs/en/navigation).

## Versioning

Documentation versioning with Docusaurus becomes simpler. When done with a new release, just simply run following command:

```shell
yarn run version ${version}
```

This preserves all markdown files in the `docs` directory and make them available as documentation for version `${version}`.
Versioned documents are placed into `website/versioned_docs/version-${version}`, where `${version}` is the version number
you supplied in the command above.

Versioned sidebars are also copied into `website/versioned_sidebars` and are named as `version-${version}-sidebars.json`.

If you want to change the documentation for a previous version, you can access files for that respective version.

For more details about versioning, refer to [Versioning](https://docusaurus.io/docs/en/versioning).
