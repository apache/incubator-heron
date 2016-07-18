[![Build Status](https://travis-ci.org/twitter/heron.svg?&branch=master)](https://travis-ci.org/twitter/heron)

# Heron

Heron is realtime analytics platform developed by Twitter. It is the direct
successor of [Apache Storm](http://storm.apache.org), built to be backwards
compatible with Storm's [topology API](http://storm.apache.org/releases/current/Tutorial.html#topologies)
but with a wide array of architectural improvements.

For more information:

* Official Heron documentation located at [heronstreaming.io](http://heronstreaming.io)
* Official Heron resources, including Conference & Journal Papers, Videos, Blog Posts and selected Press located at [Heron Resources](http://twitter.github.io/heron/docs/resources/)
* [Twitter Heron: Stream Processing at
  Scale](http://dl.acm.org/citation.cfm?id=2742788) (academic paper)
* [Twitter Heron: Stream Processing at
  Scale](https://www.youtube.com/watch?v=pUaFOuGgmco) (YouTube video)
* [Flying Faster with Twitter
  Heron](https://blog.twitter.com/2015/flying-faster-with-twitter-heron) (blog
  post)

## Update

We recently merged updates to run Heron natively using [Mesos](http://mesos.apache.org/) in [AWS](https://aws.amazon.com/), Mesos/[Aurora](http://aurora.apache.org/) in AWS, and locally on a laptop.  

We also added beta testing for [Apache YARN](https://hadoop.apache.org/docs/r2.7.1/hadoop-yarn/hadoop-yarn-site/YARN.html) support using [Apache REEF](http://reef.apache.org/), in addition to the Simple Linux Utility for Resource Management [(SLURM)](http://slurm.schedmd.com/). 

We are working to add support for [Mesosphere DC/OS](https://dcos.io/) and [Kubernetes](http://kubernetes.io/).  We will continue to post updates as we progress.

## Documentation

The official documentation for Heron is located at [heronstreaming.io](http://heronstreaming.io). To contribute to documentation, build and run the documentation locally. More information can be
found in the [documentation
README](website/README.md).
