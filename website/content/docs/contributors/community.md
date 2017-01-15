---
title: Community
---

### Contributing to Heron

Community is a critical factor to the success of Heron.  Contributions are welcome! After reviewing
the [Heron Architecture](../../concepts/architecture/), [Compiling Heron](../../developers/compiling/compiling/),
and the [Heron Codebase](../codebase/), this page covers how to contribute and, when you've made a
patch, how to submit it.

### How Can I Contribute to Heron?

In general, contributions that fix bugs or add features (as opposed to stylistic, refactoring, or
"cleanup" changes) are preferred. If you're looking for places to contribute, issues labeled
[help-wanted](https://github.com/twitter/heron/issues?q=is%3Aopen+is%3Aissue+label%3Ahelp-wanted)
are good candidates. Please check the dev list before investing a lot of time in a patch.

Continue to [Heron Architecture](../../concepts/architecture/),
[Compiling Heron](../../developers/compiling/compiling/), or [Heron Codebase](../codebase/).

### Setting up IDEA

Heron includes a script to bootstrap an IntelliJ IDEA project. The project includes support for Heron
code styles and copyright headers. To bootstrap an IDEA project run the following:

```bash
$ ./scripts/setup-intellij.sh
```

### Submitting a Patch
1. Read the Heron [governance plan](../governance) and accept the
[Twitter Contributor License Agreement](https://engineering.twitter.com/opensource/cla) (CLA).

2. Discuss your plan and design, and get agreement on our heron-dev@googlegroups.com mailing list.

3. Implement the change with unit tests and verify that all tests pass.

4. Submit a GitHub pull request that implements the feature. Clearly describe the the change in
the description. Verify that Travis CI passes.

5. Complete a code review by addressing comments of the reviewers.

6. A project committer will merge the patch to the master branch.

<!--
TODO - post commit process
TODO: links to sourcecode and dev and user groups
-->


Next: Review the [Heron Codebase](../codebase)
