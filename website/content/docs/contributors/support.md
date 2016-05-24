---
title: Support Policy
---
Core contributors generally avoid making backwards-incompatible changes and are dedicated to remain backwards-compatible with the [Apache Storm API](http://storm.apache.org/) whenever possible. Substantial unit and integration tests are performed before every release to reasonably ensure reliability at scale.

However, occasionally backwards-incompatible changes are required in order to fix bugs, to make further improvements to the system, such as improving performance or usability, or to lock down APIs that are known to be brittle.

This document gives an overview of features that are widely used and considered stable. Stability implies that changes will be backwards compatible, or that a migration path is provided.  It also covers features that are unstable. Unstable features are not yet widely used, or are already planned to change them significantly, possibly in ways that are not backwards compatible.

Before a major change happens, you can reasonably expect that advanced notice will be provided on the mailing list. Just ask on heron-users@googlegroups.com.

All undocumented features are subject to change at any time without prior notice. Features that are documented but marked *experimental* are also subject to change at any time without prior notice.

Heron relies on the Google [Bazel](http://bazel.io) build tool which is also in Beta.  Therefore, the Bazel Skylark macro and rules language (anything written in a .bzl file) is still subject to change. The Bazel Google group is in the process of migrating Google to Skylark, and expect the macro language to stabilize as part of that process.

Please help keep discover issues: report bugs and regressions in the [GitHub bugtracker](https://github.com/twitter/heron/issues). Heron core contributors will make an effort to triage all reported issues within 2 business days.

### Releases
We regularly publish [binary releases of Heron](https://github.com/twitter/heron/releases). To that end, release candidates are announced on heron-users; these are binaries that have passed all unit and integration tests. Over the next few days, regression tests are run, such as on applicable build targets at Twitter. If you have a critical project using Heron, it is recommended that you establish an automated testing process that tracks the current release candidate, and report any regressions.

If no regressions are discovered, official binaries are released after a week. However, regressions can delay the release of a release candidate. If regressions are found, corresponding cherry-picks are applied to the release candidate to fix those regressions. If no further regressions are found for two business days, but not before a week has elapsed since the first release candidate, the full binaries are released.

### Release versioning
Version 0.14.0 is our first release marking the start of our beta phase. Until version 1.0.0, we increase the MINOR version every time we reach a new milestone.
Version 1.0.0 marks the end of our beta phase; afterwards, we will label each release with a version number of the form MAJOR.MINOR.PATCH according to the [semantic version 2.0.0 document](http://semver.org/).

### Current Status
Stable
We expect the following rules and features to be stable. They are widely used within Twitter, so our internal testing should ensure that there are no major breakages.
