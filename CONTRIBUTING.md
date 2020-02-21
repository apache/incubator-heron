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

## Contributing to Heron

Discussion about Heron happens on GitHub and over the mailing list.

* GitHub: [apache/incubator-heron](https://github.com/apache/incubator-heron)
* Heron User Group: [user@heron.incubator.apache.org](https://mail-archives.apache.org/mod_mbox/heron-user/)

Community is critical to Heron. Contributions are welcomed!


## How Can I Contribute to Heron?

You can first read the following pages to have a basic understanding
of Heron:

* [Heron Architecture](https://heron.incubator.apache.org/docs/heron-architecture/)
* [Compiling Heron](https://heron.incubator.apache.org/docs/compiling-overview/)
* [Heron Codebase](https://heron.incubator.apache.org/docs/compiling-code-organization/)

Heron includes a script to bootstrap an IntelliJ IDEA project. The project includes support for Heron
code styles and copyright headers. 

To bootstrap an IDEA project run the following from the root folder of the repo::

```bash
$ ./scripts/setup-intellij.sh
```
To bootstrap an Eclipse project fun the following from the root folder of the repo:

```bash
$ ./scripts/setup-eclipse.sh
```

In general, contributions that fix bugs or add features (as opposed to stylistic, refactoring, or
"cleanup" changes) are preferred. If you're looking for places to contribute, issues with label
[help-wanted](https://github.com/apache/incubator-heron/issues?q=is%3Aopen+is%3Aissue+label%3Ahelp-wanted)
are good candidates. Please check with the [mailing list](https://mail-archives.apache.org/mod_mbox/heron-dev/)
if your patch involves lots of changes.

**If you have any question or issues about troubleshooting**,
you should post on [mailing list](https://mail-archives.apache.org/mod_mbox/heron-user/) instead
of opening GitHub issues.

### Submitting a Patch
1. Discuss your plan and design, and get agreement on
[mailing list](https://mail-archives.apache.org/mod_mbox/heron-dev/).

2. Implement proper unit tests along with your change. Verify that all tests can pass.

3. Submit a GitHub pull request that includes your change and test cases.
Describe clearly in your pull request the changes made. Verify that Travis CI passes.

4. Complete a code review by addressing the reviewer's comments.

5. A project committer will merge the patch to the master branch.