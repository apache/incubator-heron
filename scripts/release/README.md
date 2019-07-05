<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
Release Heron

# Example for releasing 0.13.0-C

./scripts/release/release.sh create 0.13.0-C b9a2997348aae2078288475c4b7d129bc14a474d 7f42fb9dc3694c77b6babdc88384c123bc8b6dd7 [MORE COMMIT ...] 
./scripts/release/release.sh push
./scripts/release/release.sh release
