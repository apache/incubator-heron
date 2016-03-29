# This file contains all the constants used
# across the tracker service.

# Version Information

API_VERSION = "1.0.0"


# Handler Constants

# Parameter Names

PARAM_CLUSTER = "cluster"
PARAM_ENVIRON = "environ"
PARAM_TOPOLOGY = "topology"
PARAM_COMPONENT = "component"
PARAM_METRICNAME = "metricname"
PARAM_INSTANCE = "instance"
PARAM_INTERVAL = "interval"
PARAM_STARTTIME = "starttime"
PARAM_ENDTIME = "endtime"
PARAM_QUERY = "query"

# These are the keys in the JSON response
# formed by the handlers.

RESPONSE_KEY_STATUS = "status"
RESPONSE_KEY_VERSION = "version"
RESPONSE_KEY_ECECUTION_TIME = "executiontime"
RESPONSE_KEY_MESSAGE = "message"
RESPONSE_KEY_RESULT = "result"

# These are the values of the status
# in the JSON repsonse.

RESPONSE_STATUS_SUCCESS = "success"
RESPONSE_STATUS_FAILURE = "failure"

# Timeout for HTTP requests.

HTTP_TIMEOUT = 5 #seconds

# default parameter - port for the tracker to listen on
DEFAULT_PORT = 8888

# default config file to read
DEFAULT_CONFIG_FILE = "localfilestateconf.yaml"

# default cluster name
DEFAULT_CLUSTER = "localhost"

