# Release Check Scripts

These are the convenience scripts for verifying a release.

Currently the scripts work on MacOS.

Development environment setup is required. Setup instrctuction can be found here: https://apache.github.io/incubator-heron/docs/developers/compiling/mac/.

## Run all release checks
```
sh ./scripts/release_check/full_release_check.sh
```

## Run individual release checks

### To run a license check with Apache Rat. Apache Rat can be downloaded here: http://ftp.wayne.edu/apache//creadur/apache-rat-0.12/apache-rat-0.12-bin.tar.gz
```
sh ./scripts/release_check/license_check.sh
```

### To compile source, into Heron release artifacts (MacOS).
```
sh ./scripts/release_check/build.sh
```

### To run a test topology locally (after build.sh is executed).
```
sh ./scripts/release_check/run_test_topology.sh
```

### To compile source into a Heron docker image (host OS: MacOS, target OS: Debian9).
```
sh ./scripts/release_check/build_docker.sh
```
