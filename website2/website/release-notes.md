
## Releases

### 0.20.5-incubating &mdash; 2021-10-22 <a id="0.20.5"></a>

## What's Changed
* links by @joshfischer1108 in https://github.com/apache/incubator-heron/pull/3694
* Update Bookkeeper to 4.13.0 and Zookeeper to 3.6.3 by @nicknezis in https://github.com/apache/incubator-heron/pull/3692
* 0.20.4 site changes by @joshfischer1108 in https://github.com/apache/incubator-heron/pull/3695
* Fix extra.links of heron-tracker by @thinker0 in https://github.com/apache/incubator-heron/pull/3696
* Adding heron s3 region into config by @Bouryu in https://github.com/apache/incubator-heron/pull/3697
* Added support for adding Kubernetes annotations to the topology pod and service by @nicknezis in https://github.com/apache/incubator-heron/pull/3699
* Add support for dynamic kubernetes labels on pod and service by @nicknezis in https://github.com/apache/incubator-heron/pull/3701
* Add ability to add Kubernetes Secrets and SecretKeyRefs by @nicknezis in https://github.com/apache/incubator-heron/pull/3702
* Updated Kryo to 5.2.0 by @nicknezis in https://github.com/apache/incubator-heron/pull/3705
* Adding missing logic to Kubernetes Scheduler to properly set the Remote Debug ports by @nicknezis in https://github.com/apache/incubator-heron/pull/3704
* Bazel upgraded to 4.1.0 by @nicknezis in https://github.com/apache/incubator-heron/pull/3703
* Update asf yaml by @joshfischer1108 in https://github.com/apache/incubator-heron/pull/3706
* Support external Helm version being set when building Helm packages by @nicknezis in https://github.com/apache/incubator-heron/pull/3708
* [HERON-3711] Setup Instructions for Intellij IDEA using the Bazel plugin. by @surahman in https://github.com/apache/incubator-heron/pull/3712
* pointing older releases to be retrieved from archive.a.o by @joshfischer1108 in https://github.com/apache/incubator-heron/pull/3714
* mirror link change by @joshfischer1108 in https://github.com/apache/incubator-heron/pull/3716
* Fix heron-downloader cannot load the jar package correctly by @zhangshaoning1 in https://github.com/apache/incubator-heron/pull/3709
* update heron-shell download handler logging by @huijunwu in https://github.com/apache/incubator-heron/pull/3718
* [HERON-3707] ConfigMap Pod Template Support by @surahman in https://github.com/apache/incubator-heron/pull/3710
* [Heron 3707]  ConfigMap Pod Template Support Documentation by @surahman in https://github.com/apache/incubator-heron/pull/3717
* Fix glog/gflags by @thinker0 in https://github.com/apache/incubator-heron/pull/3728
* Update pip-2020-resolver of PexBuilder by @thinker0 in https://github.com/apache/incubator-heron/pull/3727
* Helm: Adding option to switch the apiserver service between NodePort and ClusterIP by @windhamwong in https://github.com/apache/incubator-heron/pull/3721
* fix get_heron_dir by @thinker0 in https://github.com/apache/incubator-heron/pull/3731
* Fix get_heron_tracker_dir by @thinker0 in https://github.com/apache/incubator-heron/pull/3734
* Updated deprecated policy/v1beta1 to policy/v1 PodDisruptionBudget API by @nicknezis in https://github.com/apache/incubator-heron/pull/3737
* Bump Netty to 4.1.70 by @nicknezis in https://github.com/apache/incubator-heron/pull/3729
* [Heron-3733] CI Pipeline Failures Involving the <stmgr_unittest> by @surahman in https://github.com/apache/incubator-heron/pull/3735
* update filename in integration_test http_server by @huijunwu in https://github.com/apache/incubator-heron/pull/3739
* Restart a Topology on Kubernetes by @surahman in https://github.com/apache/incubator-heron/pull/3740
* [Heron-3723] Add support for Persistent Volumes for stateful storage by @surahman in https://github.com/apache/incubator-heron/pull/3725
* [Heron-3724] Separate the Manager and Executors. by @surahman in https://github.com/apache/incubator-heron/pull/3741
* Kazoo version bump for Python 3.8 by @windhamwong in https://github.com/apache/incubator-heron/pull/3743
* Fix dependency by @thinker0 in https://github.com/apache/incubator-heron/pull/3746
* adding new committer by @joshfischer1108 in https://github.com/apache/incubator-heron/pull/3748
* Updated to fix missing anticrlf dependency by @nicknezis in https://github.com/apache/incubator-heron/pull/3749
* [Heron-3723] Add support for Empty Dir, Host Path, and NFS via CLI by @surahman in https://github.com/apache/incubator-heron/pull/3747
* Kubernetes V1Controller Cleanup by @surahman in https://github.com/apache/incubator-heron/pull/3752
* Added Docker to Vagrant VM init script by @nicknezis in https://github.com/apache/incubator-heron/pull/3756
* Updated External JVM Rules plugin by @nicknezis in https://github.com/apache/incubator-heron/pull/3753
* Updated Kubernetes client library to 14.0.0 by @nicknezis in https://github.com/apache/incubator-heron/pull/3754
* Updated Netty to 4.1.72 by @nicknezis in https://github.com/apache/incubator-heron/pull/3755
* Updated Dhalion to 0.2.6 by @nicknezis in https://github.com/apache/incubator-heron/pull/3757
* Helm chart cleanup by @nicknezis in https://github.com/apache/incubator-heron/pull/3758
* HeronPy 0.20.5 release prep cleanup by @nicknezis in https://github.com/apache/incubator-heron/pull/3759
* libunwind 1.5.0 Upgrade by @surahman in https://github.com/apache/incubator-heron/pull/3760
* Ubuntu 22.04 Support by @surahman in https://github.com/apache/incubator-heron/pull/3761
* Upgrade to Bazel 4.2.2 by @nicknezis in https://github.com/apache/incubator-heron/pull/3764
* Updated Google Test which now supports Bazel by @nicknezis in https://github.com/apache/incubator-heron/pull/3765
* Fixed stmgr unittest segfault issue on macOS by @nicknezis in https://github.com/apache/incubator-heron/pull/3767
* Upgrade WORKSPACE python packages for python3.8 support by @Code0x58 in https://github.com/apache/incubator-heron/pull/3646
* Fixing an incorrect use of KubeVersion in the Helm Chart.yml by @nicknezis in https://github.com/apache/incubator-heron/pull/3775
* Update to fix Heron UI Jinja2 issue by @nicknezis in https://github.com/apache/incubator-heron/pull/3777
* Added missing packing plan handler by @nicknezis in https://github.com/apache/incubator-heron/pull/3783
* Removing old Tornado asynchronous annotations by @nicknezis in https://github.com/apache/incubator-heron/pull/3781
* Add legacy CentOS7 by @thinker0 in https://github.com/apache/incubator-heron/pull/3772
* Change centos to RockyLinux-8 by @thinker0 in https://github.com/apache/incubator-heron/pull/3773
* Fix conflict Rocky8 by @thinker0 in https://github.com/apache/incubator-heron/pull/3787
* Fix git url by @thinker0 in https://github.com/apache/incubator-heron/pull/3794
* Remove unnecessary Bookkeeper format init container from Helm chart by @nicknezis in https://github.com/apache/incubator-heron/pull/3795
* Change log4j to slf4j of Logging by @thinker0 in https://github.com/apache/incubator-heron/pull/3791
* Remove fixed young generation heap space of instance by @thinker0 in https://github.com/apache/incubator-heron/pull/3789
* Support Runtime jdk17 by @thinker0 in https://github.com/apache/incubator-heron/pull/3792
* Update build-docker.sh - Removing debug flag by @windhamwong in https://github.com/apache/incubator-heron/pull/3799
* Upgrade protobuf to 3.16.1 CVE-2021-22569 by @thinker0 in https://github.com/apache/incubator-heron/pull/3797
* Adding Bazel Platforms support by @nicknezis in https://github.com/apache/incubator-heron/pull/3779
* Removing prevously missed refs to removed files by @nicknezis in https://github.com/apache/incubator-heron/pull/3805
* Updated to fix MacOS stylecheck RuntimeError by @nicknezis in https://github.com/apache/incubator-heron/pull/3803
* Update cppcheck to 2.7 by @nicknezis in https://github.com/apache/incubator-heron/pull/3804
* Updated rules_pkg build dependency to 0.6.0 by @nicknezis in https://github.com/apache/incubator-heron/pull/3806
* Add logback.xml by @thinker0 in https://github.com/apache/incubator-heron/pull/3807
* Add SLF4JBridgeHandler by @thinker0 in https://github.com/apache/incubator-heron/pull/3809
* Bump minimist from 1.2.5 to 1.2.6 in /website2/website by @dependabot in https://github.com/apache/incubator-heron/pull/3811
* Replace Helm chart's generic Bookkeeper platform property with specific properties by @wromansky in https://github.com/apache/incubator-heron/pull/3798
* Bump lodash from 4.17.15 to 4.17.21 in /website2/website by @dependabot in https://github.com/apache/incubator-heron/pull/3812
* Fix for missing physical plan in UI by @nicknezis in https://github.com/apache/incubator-heron/pull/3786
* Bump url-parse from 1.4.7 to 1.5.10 in /website2/website by @dependabot in https://github.com/apache/incubator-heron/pull/3813
* Change ByteSize to ByteSizeLong for Protobuf by @thinker0 in https://github.com/apache/incubator-heron/pull/3816
* Bump prismjs from 1.19.0 to 1.27.0 in /website2/website by @dependabot in https://github.com/apache/incubator-heron/pull/3815
* Support legacy api of HealthManger by @thinker0 in https://github.com/apache/incubator-heron/pull/3818
* Fix HealthManager by @thinker0 in https://github.com/apache/incubator-heron/pull/3819
* Bump async from 2.6.3 to 2.6.4 in /website2/website by @dependabot in https://github.com/apache/incubator-heron/pull/3822
* Updated Netty to 4.1.76.Final by @nicknezis in https://github.com/apache/incubator-heron/pull/3823
* Upgrade Kryo to 5.3.0 by @nicknezis in https://github.com/apache/incubator-heron/pull/3824
* Joshfischer/3774/pom dependencies by @joshfischer1108 in https://github.com/apache/incubator-heron/pull/3778
* Added License file to published Jars by @nicknezis in https://github.com/apache/incubator-heron/pull/3827
* Update Debian Docker images from 10 to 11 by @nicknezis in https://github.com/apache/incubator-heron/pull/3828
* [3821] Remove deprecated Host Path, NFS, and EBS support for Kubernetes by @surahman in https://github.com/apache/incubator-heron/pull/3825
* Update rules-scala of Scala by @thinker0 in https://github.com/apache/incubator-heron/pull/3832
* update year in NOTICE, adding ASF headers to missed files by @joshfischer1108 in https://github.com/apache/incubator-heron/pull/3834
* Added to show the number of instances in the topology list UI. by @thinker0 in https://github.com/apache/incubator-heron/pull/3831
* Rearrange values.yaml to better style by @windhamwong in https://github.com/apache/incubator-heron/pull/3835
* Reduce the DEBUG log by @thinker0 in https://github.com/apache/incubator-heron/pull/3836
* Removed random long in filename which caused leaking in upload storage by @nicknezis in https://github.com/apache/incubator-heron/pull/3838
* Upgraded Bookkeeper to 4.14.5 by @nicknezis in https://github.com/apache/incubator-heron/pull/3837
* Upgrade to Zookeeper 3.8.0 by @nicknezis in https://github.com/apache/incubator-heron/pull/3840
* Added missing epel-release repo in Rocky8 Dist image Dockerfile by @nicknezis in https://github.com/apache/incubator-heron/pull/3842
* Fix Update of topology by @thinker0 in https://github.com/apache/incubator-heron/pull/3830
* Remove stray space characters between \ and \n by @jmtd in https://github.com/apache/incubator-heron/pull/3843
* Updates to fix issues with the Heron API dependencies by @nicknezis in https://github.com/apache/incubator-heron/pull/3844

## New Contributors
* @Bouryu made their first contribution in https://github.com/apache/incubator-heron/pull/3697
* @surahman made their first contribution in https://github.com/apache/incubator-heron/pull/3712
* @zhangshaoning1 made their first contribution in https://github.com/apache/incubator-heron/pull/3709
* @wromansky made their first contribution in https://github.com/apache/incubator-heron/pull/3798
* @jmtd made their first contribution in https://github.com/apache/incubator-heron/pull/3843


### 0.20.4-incubating &mdash; 2019-08-27 <a id="0.20.4"></a>

- 83a742f (tag: 0.20.4-incubating-rc1, upstream/master) Adding disclaimers to release artifacts [#3689](https://github.com/apache/incubator-heron/pull/3689)
- db5c992 (origin/master, origin/HEAD) Use the proper featured Zookeeper config generation script [#3688](https://github.com/apache/incubator-heron/pull/3688)
- 103cf19 (upstream/ubuntu20.04-arm64) updatedocandlink [#3685](https://github.com/apache/incubator-heron/pull/3685)
- c23dba7 Updated to Kryo 5.0.4 [#3683](https://github.com/apache/incubator-heron/pull/3683)
- 8ff6390 Fixing incorrect article title [#3682](https://github.com/apache/incubator-heron/pull/3682)
- 63fe9fa Grammar fix [#3681](https://github.com/apache/incubator-heron/pull/3681)
- c2d7f14 Remove cppunit-dev dependency [#3680](https://github.com/apache/incubator-heron/pull/3680)
- 2956293 Decrease the minimum requirement of RAM of single instance to 16MB in Round Robin algorithm [#3678](https://github.com/apache/incubator-heron/pull/3678)
- d4d3ccc Removing unused Bookkeeper hostPort that causes port conflict [#3677](https://github.com/apache/incubator-heron/pull/3677)
- a8eb752 Specifying explicit transitive dependency for uvloop due to Python version conflict [#3675](https://github.com/apache/incubator-heron/pull/3675)
- a945338 Fixed website style issue [#3673](https://github.com/apache/incubator-heron/pull/3673)
- 43fa2ff Joshfischer/add download links [#3671](https://github.com/apache/incubator-heron/pull/3671)
- bfdbd43 Reverting Glog back to 0.3.5 [#3672](https://github.com/apache/incubator-heron/pull/3672)
- a51c822 Use Build Kit docker feature to shrink the Docker image size [#3669](https://github.com/apache/incubator-heron/pull/3669)
- b161317 Changed heartbeat log statement to debug [#3667](https://github.com/apache/incubator-heron/pull/3667)
- 7a75e51 Removing defaulted HeapDumpOnOutOfMemory [#3665](https://github.com/apache/incubator-heron/pull/3665)
- f11ebc3 Added CLI submit config toggle for verbose GC logging [#3663](https://github.com/apache/incubator-heron/pull/3663)
- fdf3430 Add sum/max/min reducers in Scala [#3133](https://github.com/apache/incubator-heron/pull/3133)
- 1a0d198 Updated K8s Scheduler to set Resource Limits and Requests [#3664](https://github.com/apache/incubator-heron/pull/3664)
- 0f0ba18 explicit unreleased code [#3658](https://github.com/apache/incubator-heron/pull/3658)
- 6edbee7 Allow localhost graceful shutdown [#3650](https://github.com/apache/incubator-heron/pull/3650)
- b363319 GLog updated to 0.4.0. Also some C++ code cleanup to remove warnings [#3655](https://github.com/apache/incubator-heron/pull/3655)
- 6e83c96 Updated Kryo to 5.0.3 [#3657](https://github.com/apache/incubator-heron/pull/3657)
- 1a8fc39 update notice year [#3656](https://github.com/apache/incubator-heron/pull/3656)
- 9af45c7 Improving Kubernetes scheduler logic [#3653](https://github.com/apache/incubator-heron/pull/3653)
- 0025251 Bumping Bazel to 3.7.2 [#3654](https://github.com/apache/incubator-heron/pull/3654)
- 01abf01 Add Prometheus metrics rules [#3651](https://github.com/apache/incubator-heron/pull/3651)



### 0.20.0-incubating &mdash; 2018-11-21 <a id="0.20.0"></a>

This is the first release of Heron as an Apache Incubating Project

- Fix annotation processor (#3038)
- Refactor heron_executor to make it easier to customize (#3043)
- Remove libunwind patches (#3041)
- cleanup README (#3037)
- Fix Topology-API window documentation (#3039)
- Nwang/update thirdparty license (#3042)
- Fix 'Copyright 2015 The Heron Authors' in scripts (#3036)



