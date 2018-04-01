[![Build Status](https://travis-ci.org/apache/incubator-heron.svg?&branch=master)](https://travis-ci.org/apache/incubator-heron)

![logo](website/static/img/HeronTextLogo.png)

Heron is realtime analytics platform developed by Twitter. It is the direct
successor of [Apache Storm](http://storm.apache.org), built to be backwards
compatible with Storm's [topology API](http://storm.apache.org/releases/current/Tutorial.html#topologies)
but with a wide array of architectural improvements.

http://incubator.apache.org/projects/heron.html

https://heron.incubator.apache.org (coming soon...)

### Documentation
http://heronstreaming.io (official until)

#### Heron Requirements:
 * Java JDK 1.8
 * Python 2.7
 * Bazel


## Contact

#### Mailing lists

Heron User Google Group: [heron-users@googlegroups.com](https://groups.google.com/forum/#!forum/heron-users)

Heron on Twitter: [@heronstreaming](https://twitter.com/heronstreaming)


| Name                                                                      | Scope                           |                                                                |                                                                    |                                                                           |
|:--------------------------------------------------------------------------|:--------------------------------|:---------------------------------------------------------------|:-------------------------------------------------------------------|:--------------------------------------------------------------------------|
| [user@heron.incubator.apache.org](mailto:user@heron.incubator.apache.org) | User-related discussions        | [Subscribe](mailto:user-subscribe@heron.incubator.apache.org)  | [Unsubscribe](mailto:user-unsubscribe@heron.incubator.apache.org)  | [Archives](http://mail-archives.apache.org/mod_mbox/incubator-heron-user/)|
| [dev@heron.incubator.apache.org](mailto:dev@heron.incubator.apache.org)   | Development-related discussions | [Subscribe](mailto:dev-subscribe@heron.incubator.apache.org)   | [Unsubscribe](mailto:dev-unsubscribe@heron.incubator.apache.org)   | [Archives](http://mail-archives.apache.org/mod_mbox/incubator-heron-dev/) |

#### Slack

heron slack channel at https://heronstreaming.slack.com/

You can self-register at http://heronstreaming.herokuapp.com/

#### Meetup Group
https://www.meetup.com/Apache-Heron-Bay-Area


## For more information:

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


## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
