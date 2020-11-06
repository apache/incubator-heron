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
Vagrant VM for CI and debugging
=================================
Running `vagrant up primary` will bring up an environment similar to the one used by Travis for CI. If the build fails, it can be inspected by entering the machine with `vagrant ssh primary`. When you're down with the VM, you can clean up with `vagrant destroy -f`.

The advantage of this is you don't need to worry about the potential environment pollution, and others can reproduce the results from other platforms.
