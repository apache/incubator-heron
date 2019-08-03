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
Semver -- python module for semantic versioning
===============================================

![Travis CI](https://travis-ci.org/k-bx/python-semver.svg?branch=master)

Simple module for comparing versions as noted at [semver.org](http://semver.org/).

This module provides just couple of functions, main of which are:

```python
>>> import semver
>>> semver.compare("1.0.0", "2.0.0")
-1
>>> semver.compare("2.0.0", "1.0.0")
1
>>> semver.compare("2.0.0", "2.0.0")
0
>>> semver.match("2.0.0", ">=1.0.0")
True
>>> semver.match("1.0.0", ">1.0.0")
False
>>> semver.format_version(3, 4, 5, 'pre.2', 'build.4')
'3.4.5-pre.2+build.4'
>>> semver.bump_major("3.4.5")
'4.0.0'
>>> semver.bump_minor("3.4.5")
'3.5.0'
>>> semver.bump_patch("3.4.5")
'3.4.6'
>>> semver.max_ver("1.0.0", "2.0.0")
'2.0.0'
>>> semver.min_ver("1.0.0", "2.0.0")
'1.0.0'
```

Installation
------------

For Python 2:

```
pip install semver
```

For Python 3:

```
pip3 install semver
```

Homepage at PyPi: https://pypi.python.org/pypi/semver
