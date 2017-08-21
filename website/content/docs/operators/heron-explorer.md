---
title: Heron Explorer
---

The **Heron Explorer** is a CLI tool that you can use

In order to use the Heron Explorer, the [Heron Tracker](../heron-tracker) will need to be running. If you've [installed the Tracker](../../getting-started), you can start it up using just one command:

```shell
$ heron-tracker
```

## Commands

Command | Action | Arguments
:-------|:-------|:---------
`clusters` | List all currently available Heron clusters |
`components` | Display information about a topology's components | `[cluster]/[role]/[env] [topology-name] [options]`