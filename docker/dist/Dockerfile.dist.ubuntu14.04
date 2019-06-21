#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

FROM ubuntu:14.04

RUN apt-get -y update && apt-get -y install \
    python \
    unzip \
    software-properties-common \
    supervisor \
    curl

RUN add-apt-repository ppa:openjdk-r/ppa && apt-get -y update && \
    apt-get -y install openjdk-8-jdk

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN update-ca-certificates -f

ADD artifacts /heron

WORKDIR /heron

# run heron installer
RUN /heron/heron-install.sh

RUN ln -s /usr/local/heron/dist/heron-core /heron \
    && mkdir -p /heron/heron-tools \
    && ln -s /usr/local/heron/bin /heron/heron-tools \
    && ln -s /usr/local/heron/conf /heron/heron-tools \
    && ln -s /usr/local/heron/dist /heron/heron-tools \
    && ln -s /usr/local/heron/lib /heron/heron-tools \
    && ln -s /usr/local/heron/release.yaml /heron/heron-tools \
    && ln -s /usr/local/heron/examples /heron \
    && ln -s /usr/local/heron/release.yaml /heron

ENV HERON_HOME /heron/heron-core/

# install zookeeper
ARG ZK_DIST=zookeeper-3.4.10

RUN curl -O "https://archive.apache.org/dist/zookeeper/$ZK_DIST/$ZK_DIST.tar.gz" \
    && tar -xzf /heron/$ZK_DIST.tar.gz -C /opt \
    && rm -r /heron/$ZK_DIST.tar.gz \
    && mv /opt/$ZK_DIST /opt/zookeeper \
    && rm -rf /heron/heron-install.sh \
    && rm -rf /opt/zookeeper/CHANGES.txt \
    /opt/zookeeper/README.txt \
    /opt/zookeeper/NOTICE.txt \
    /opt/zookeeper/CHANGES.txt \
    /opt/zookeeper/README_packaging.txt \
    /opt/zookeeper/build.xml \
    /opt/zookeeper/config \
    /opt/zookeeper/contrib \
    /opt/zookeeper/dist-maven \
    /opt/zookeeper/docs \
    /opt/zookeeper/ivy.xml \
    /opt/zookeeper/ivysettings.xml \
    /opt/zookeeper/recipes \
    /opt/zookeeper/src \
    /opt/zookeeper/$ZK_DIST.jar.asc \
    /opt/zookeeper/$ZK_DIST.jar.md5 \
    /opt/zookeeper/$ZK_DIST.jar.sha1

ADD dist/conf/zookeeper.conf /opt/zookeeper/conf/zookeeper.conf
ADD dist/conf/sandbox.conf /etc/supervisor/conf.d/

RUN mkdir -p /opt/zookeeper/scripts
ADD dist/scripts /opt/zookeeper/scripts
RUN chmod +x /opt/zookeeper/scripts/generate-zookeeper-config.sh \
    && chmod +x /opt/zookeeper/scripts/zookeeper-ruok.sh \
    && chmod +x /opt/zookeeper/scripts/start-zookeeper.sh

CMD ["supervisord", "-n"]
