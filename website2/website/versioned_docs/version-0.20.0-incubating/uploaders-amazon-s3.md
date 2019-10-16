---
id: version-0.20.0-incubating-uploaders-amazon-s3
title: Amazon S3
sidebar_label: Amazon S3
original_id: uploaders-amazon-s3
---
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

If you are running in Amazon AWS, Heron provides out of the box uploader for S3,
the object storage. S3 uploader is useful when the topologies run in a distributed
cluster of Amazon EC2 compute instances. Since S3 replicates the data, it provides
a scalable mechanism for distributing the user topology jars.

### S3 Uploader Configuration

You can make Heron use S3 uploader by modifying the `uploader.yaml` config file specific
for the Heron cluster. You'll need to specify the following for each cluster:

* `heron.class.uploader` --- Indicate the uploader class to be loaded. You should set this
to `org.apache.heron.uploader.s3.S3Uploader`

* `heron.uploader.s3.bucket` --- Specifies the S3 bucket where the topology jar should be
uploaded.

* `heron.uploader.s3.access_key` --- Specify the access key of the AWS account that has
write access to the bucket

* `heron.uploader.s3.secret_key` --- Specify the secret access of the AWS account that has
write access to the bucket

### Example S3 Uploader Configuration

Below is an example configuration (in `uploader.yaml`) for a S3 uploader:

```yaml
# uploader class for transferring the topology jar/tar files to storage
heron.class.uploader: org.apache.heron.uploader.s3.S3Uploader

# S3 region bucket is created.  Must specify.
heron.uploader.s3.region: us-east-1

# S3 bucket to put the jar file into
heron.uploader.s3.bucket: heron-topologies-company-com

# AWS access key
heron.uploader.s3.access_key: access_key

# AWS secret access key
heron.uploader.s3.secret_key: secret_access_key
```
