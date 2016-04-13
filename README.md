[![Build Status](https://travis-ci.com/twitter/heron.svg?token=woUsyvMQCWdrt9jhGR7x&branch=master)](https://travis-ci.com/twitter/heron)

# Heron

Heron is realtime analytics platform developed by Twitter. It is the direct
successor of [Apache Storm](http://storm.apache.org), built to be backwards
compatible with Storm's [topology API](http://storm.apache.org/tutorial.html)
but with a wide array of architectural improvements. For more on the
architecture and design goals of Heron, see Heron's
[documentation](#Heron-Documentation), especially [The Architecture of
Heron](docs/concepts/architecture.md) and [Heron
Topologies](docs/concepts/topologies.md).

For information beyond the documentation:

* [Twitter Heron: Stream Processing at
  Scale](http://dl.acm.org/citation.cfm?id=2742788) (academic paper)
* [Twitter Heron: Stream Processing at
  Scale](https://www.youtube.com/watch?v=pUaFOuGgmco) (YouTube video)
* [Flying Faster with Twitter
  Heron](https://blog.twitter.com/2015/flying-faster-with-twitter-heron) (blog
  post)

## Update

Currently, we are working on ensuring that Heron is easy to install and run in a
Mesos Cluster in AWS, Mesos/Aurora in AWS, and locally on a laptop. We will post updates as we progress.

# Heron Documentation

Heron's documentation was built using the following components:

* [Hugo](http://gohugo.io) --- Static site generator
* [GulpJS](http://gulpjs.com) --- Build tool for static assets
* [Twitter Bootstrap](http://getbootstrap.com) --- CSS and JavaScript

## Documentation Setup

Running the Heron documentation locally requires that you have the following
installed:

* [Homebrew](http://brew.sh)
* [npm](https://www.npmjs.com)

If you have both Homebrew and npm installed:

```bash
$ cd website
$ make setup
```

This will install Hugo, Gulp, and all the necessary Gulp plugins.

## Running the Docs Locally

```bash
$ make serve
```

This will run the docs locally on `localhost` port 1313.
