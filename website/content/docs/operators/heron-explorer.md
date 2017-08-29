---
title: Heron Explorer
---

The **Heron Explorer** is a CLI tool that you can use to

> #### The Heron Explorer vs. Heron CLI
> There are two important differences between the Heron Explorer and [Heron CLI](../heron-cli): unlike Heron CLI, the Heron Explorer (a) requires the [Heron Tracker](../heron-tracker) and (b) performs read-only, observation-oriented commands (rather than actions like submitting, activating, and killing topologies).

In order to use the Heron Explorer, the [Heron Tracker](../heron-tracker) will need to be running. If you've [installed the Tracker](../../getting-started), you can start it up using just one command:

```shell
$ heron-tracker
```

## Commands



Command | Action | Arguments
:-------|:-------|:---------
`clusters` | List all currently available Heron clusters |
`components` | Display information about a topology's components | `[cluster]/[role]/[env] [topology-name] [options]`
`metrics` | Display metrics for a topology | `[cluster]/[role]/[env] [topology-name] [options]`