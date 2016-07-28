---
title: Community
---


## Contributing to Heron

Discussion about Heron happens on GitHub and over mailing list.

* GitHub: [twitter/heron](https://github.com/twitter/heron)
* Heron User Google Group: [heron-users@googlegroups.com](https://groups.google.com/forum/#!forum/heron-users)
* Heron on Twitter: [@heronstreaming](https://twitter.com/heronstreaming)

Community is critical to Heron. Contributions are welcomed!


## How Can I Contribute to Heron?

You can first read the following pages to have a basic understanding
of Heron:

* [Heron Architecture](../../concepts/architecture/)
* [Compiling Heron](../../developers/compiling/compiling/)
* [Heron Codebase](../codebase/)

Heron includes a script to bootstrap an IntelliJ IDEA project. The project includes support for Heron
code styles and copyright headers. To bootstrap an IDEA project run the following:

```bash
$ ./scripts/setup-intellij.sh
```

In general, contributions that fix bugs or add features (as opposed to stylistic, refactoring, or
"cleanup" changes) are preferred. If you're looking for places to contribute, issues with label
[help-wanted](https://github.com/twitter/heron/issues?q=is%3Aopen+is%3Aissue+label%3Ahelp-wanted)
are good candidates. Please check with [mailing list](https://groups.google.com/forum/#!forum/heron-users)
if your patch involves lots of changes.

**If you have any question or issues about troubleshooting,
you should post on [mailing list](https://groups.google.com/forum/#!forum/heron-users) instead
of opening GitHub issues.**

### Submitting a Patch
1. Read the Heron [governance plan](../governance) and accept the
[Twitter Contributor License Agreement](https://engineering.twitter.com/opensource/cla) (CLA).

2. Discuss your plan and design, and get agreement on
[mailing list](https://groups.google.com/forum/#!forum/heron-users).

3. Implement proper unit tests along with your change. Verify that all tests can pass.

4. Submit a GitHub pull request that includes your change and test cases.
Describe clearly your pull request the change. Verify that Travis CI passes.

5. Complete a code review by addressing reviewers's comments.

6. A project committer will merge the patch to the master branch.

<!--
TODO - post commit process
TODO: links to sourcecode and dev and user groups
-->


Next: Review the [Heron Codebase](../codebase)
