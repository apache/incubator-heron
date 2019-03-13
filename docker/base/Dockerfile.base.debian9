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

FROM openjdk:8-jdk-slim

RUN apt-get -y update && apt-get -y install \
    netcat \
    python \
    unzip \
    curl \
    supervisor && \
    apt-get clean all && \
    rm -rf /var/lib/apt/lists/*

# install zookeeper
ARG ZK_DIST=zookeeper-3.4.10

RUN curl -O "https://archive.apache.org/dist/zookeeper/$ZK_DIST/$ZK_DIST.tar.gz" \
    && tar -xzf $ZK_DIST.tar.gz -C /opt \
    && rm -r $ZK_DIST.tar.gz \
    && mv /opt/$ZK_DIST /opt/zookeeper \
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

ADD base/conf/zookeeper.conf /opt/zookeeper/conf/zookeeper.conf
ADD base/conf/sandbox.conf /etc/supervisor/conf.d/

RUN mkdir -p /opt/zookeeper/scripts
ADD base/scripts /opt/zookeeper/scripts
RUN chmod +x /opt/zookeeper/scripts/generate-zookeeper-config.sh && \
    chmod +x /opt/zookeeper/scripts/zookeeper-ruok.sh && \
    chmod +x /opt/zookeeper/scripts/start-zookeeper.sh && \
    chmod +x /opt/zookeeper/scripts/wait-for-zookeeper.sh
