/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


/**
 * Keys for configuration of overrides (via properties file).
 */
public class ConfigKeys {
    
    public static final String STREAM_NAME_KEY = "streamName";
    
    public static final String INITIAL_POSITION_IN_STREAM_KEY = "initialPositionInStream";

    public static final String RECORD_RETRY_LIMIT = "recordRetryLimit";

    public static final String REGION_KEY = "region";

    public static final String ZOOKEEPER_ENDPOINT_KEY = "zookeeperEndpoint";
    
    public static final String ZOOKEEPER_PREFIX_KEY = "zookeeperPrefix";

    public static final String TOPOLOGY_NAME_KEY = "topologyName";
}
