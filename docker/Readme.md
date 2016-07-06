# Docker

## To compile source using a docker container:
```
./docker/build-artifacts.sh <platform> <version_string> [source-tarball] <output-directory>
# e.g.  ./docker/build-artifacts.sh ubuntu14.04 testbuild ~/heron-release
```

## To build docker containers for running heron daemons:
```
./docker/build-docker.sh <platform> <version_string> <output-directory>
# e.g. ./docker/build-docker.sh ubuntu14.04 testbuild ~/heron-release
```

### To run docker containers for local dev work:
```
./docker/start-docker.sh heron:<version_string>-<platform>
# e.g. ./docker/start-docker.sh heron:testbuild-ubuntu14.04
```
### To stop docker containers for local dev work:
```
./docker/stop-docker.sh
```
### To submit/activate/kill a topology:
```
#To submit a topology:
        docker exec heron_executor_1 heron submit local /usr/local/heron/examples/heron-examples.jar com.twitter.heron.examples.ExclamationTopology ExclamationTopology --deploy-deactivated
#To activate a topology:
        docker exec -it heron_executor_1 heron activate local ExclamationTopology
#To kill a topology:
        docker exec -it heron_executor_1 heron kill local ExclamationTopology
```
## To access heron ui:
* determine the IP of your docker host
* navigate to http://<your docker host>:8889 in your web browser
