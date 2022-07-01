#!/bin/bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Updates maven POM artifacts with version.

# Usage
# ./maven/maven-pom-version.sh VERSION
# Example
# ./maven/maven-pom-version.sh 0.14.1

if [ "$1" = "" ]; then
    echo "ERROR: heron version missing. Usage './maven/maven-pom-version.sh VERSION' "
    exit 1
fi

cat ./maven/heron-no-kryo.template.pom | \
    sed "s/VERSION/$1/g" | \
    sed "s/ARTIFACT_ID/heron-api/g" | \
    sed "s/NAME/heron-api/g" | \
    sed "s/DESCRIPTION/Heron API/g" | \
    awk '{gsub("DEPS", "<dependency>\
                          <groupId>org.apache.commons</groupId>\
                            <artifactId>commons-lang3</artifactId>\
                            <version>3.12.0</version>\
                        </dependency>\
                        <dependency>\
                          <groupId>com.google.protobuf</groupId>\
                            <artifactId>protobuf-java</artifactId>\
                            <version>3.16.1</version>\
                        </dependency>\
                        <dependency>\
                          <groupId>javax.xml.bind</groupId>\
                            <artifactId>jaxb-api</artifactId>\
                            <version>2.3.0</version>\
                        </dependency>\
                        <dependency>\
                          <groupId>org.slf4j</groupId>\
                            <artifactId>jul-to-slf4j</artifactId>\
                            <version>1.7.36</version>\
                        </dependency>", $0); print}' | \
    xmllint --format - \
    >> ./heron-api-$1.pom

cat ./maven/heron-no-kryo.template.pom | \
    sed "s/VERSION/$1/g" | \
    sed "s/ARTIFACT_ID/heron-spi/g" | \
    sed "s/NAME/heron-spi/g" | \
    sed "s/DESCRIPTION/Heron SPI/g" | \
    awk '{gsub("DEPS", "<dependency>\
                            <groupId>org.apache.heron</groupId>\
                            <artifactId>heron-api</artifactId>\
                            <version>HERON_API_VERSION</version>\
                        </dependency>\
                        <dependency>\
                            <groupId>com.google.protobuf</groupId>\
                            <artifactId>protobuf-java</artifactId>\
                            <version>3.19.1</version>\
                        </dependency>\
                         <dependency>\
                             <groupId>com.google.guava</groupId>\
                             <artifactId>guava</artifactId>\
                             <version>23.6-jre</version>\
                         </dependency>\
                         <dependency>\
                             <groupId>org.yaml</groupId>\
                             <artifactId>snakeyaml</artifactId>\
                             <version>1.29</version>\
                         </dependency>", $0); print}' | \
    sed "s/HERON_API_VERSION/$1/g" | \
    xmllint --format - \
    >> ./heron-spi-$1.pom

cat ./maven/heron-with-kryo.template.pom | \
    sed "s/VERSION/$1/g" | \
    sed "s/ARTIFACT_ID/heron-storm/g" | \
    sed "s/NAME/heron-storm/g" | \
    sed "s/DESCRIPTION/Heron Storm/g" | \
    awk '{gsub("DEPS", "<dependency>\
                            <groupId>org.apache.heron</groupId>\
                            <artifactId>heron-api</artifactId>\
                            <version>HERON_API_VERSION</version>\
                        </dependency>\
                        <dependency>\
                        <groupId>org.apache.commons</groupId>\
                            <artifactId>commons-lang3</artifactId>\
                            <version>3.12.0</version>\
                        </dependency>\
                        <dependency>\
                           <groupId>org.yaml</groupId>\
                           <artifactId>snakeyaml</artifactId>\
                           <version>1.29</version>\
                         </dependency>\
                         <dependency>\
                             <groupId>com.googlecode.json-simple</groupId>\
                             <artifactId>json-simple</artifactId>\
                             <version>1.1</version>\
                         </dependency>", $0); print}' | \
    sed "s/HERON_API_VERSION/$1/g" | \
    xmllint --format - \
    >> ./heron-storm-$1.pom

cat ./maven/heron-with-kryo.template.pom | \
    sed "s/VERSION/$1/g" | \
    sed "s/ARTIFACT_ID/heron-simulator/g" | \
    sed "s/NAME/heron-simulator/g" | \
    sed "s/DESCRIPTION/Heron Simulator/g" | \
    awk '{gsub("DEPS", "<dependency>\
                            <groupId>org.apache.heron</groupId>\
                            <artifactId>heron-api</artifactId>\
                            <version>HERON_API_VERSION</version>\
                        </dependency>\
                        <dependency>\
                          <groupId>org.apache.commons</groupId>\
                            <artifactId>commons-lang3</artifactId>\
                            <version>3.12.0</version>\
                        </dependency>\
                        <dependency>\
                           <groupId>org.yaml</groupId>\
                           <artifactId>snakeyaml</artifactId>\
                           <version>1.29</version>\
                        </dependency>\
                        <dependency>\
                            <groupId>org.glassfish.jersey.media</groupId>\
                             <artifactId>jersey-media-jaxb</artifactId>\
                              <version>2.25.1</version>\
                        </dependency>", $0); print}' | \
    sed "s/HERON_API_VERSION/$1/g" | \
    xmllint --format - \
    >> ./heron-simulator-$1.pom

cat ./maven/heron-kafka.template.pom | \
    sed "s/VERSION/$1/g" | \
    sed "s/ARTIFACT_ID/heron-kafka-spout/g" | \
    sed "s/NAME/heron-kafka-spout/g" | \
    sed "s/DESCRIPTION/Heron Kafka Spout/g" | \
    awk '{gsub("DEPS", "<dependency>\
                              <groupId>org.slf4j</groupId>\
                              <artifactId>slf4j-jdk14</artifactId>\
                              <version>1.7.30</version>\
                          </dependency>\
                           <dependency>\
                               <groupId>org.slf4j</groupId>\
                               <artifactId>slf4j-api</artifactId>\
                               <version>1.7.30</version>\
                           </dependency>", $0); print}' | \
    xmllint --format - \
    >> ./heron-kafka-spout-$1.pom

cat ./maven/heron-kafka.template.pom | \
    sed "s/VERSION/$1/g" | \
    sed "s/ARTIFACT_ID/heron-kafka-bolt/g" | \
    sed "s/NAME/heron-kafka-bolt/g" | \
    sed "s/DESCRIPTION/Heron Kafka Bolt/g" | \
    awk '{gsub("DEPS", "<dependency>\
                            <groupId>org.slf4j</groupId>\
                            <artifactId>slf4j-jdk14</artifactId>\
                            <version>1.7.30</version>\
                        </dependency>\
                         <dependency>\
                             <groupId>org.slf4j</groupId>\
                             <artifactId>slf4j-api</artifactId>\
                             <version>1.7.30</version>\
                         </dependency>", $0); print}' | \
    xmllint --format - \
    >> ./heron-kafka-bolt-$1.pom
