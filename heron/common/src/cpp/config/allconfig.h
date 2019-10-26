/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

////////////////////////////////////////////////////////////////////////////////
//
// config.h
//
// Lists all header files in this dir for easy include
///////////////////////////////////////////////////////////////////////////////

#if !defined(__SP_SVCS_CONFIG_CONFIG_H)
#define __SP_SVCS_CONFIG_CONFIG_H

#include "config/yaml-file-reader.h"
#include "config/topology-config-vars.h"
#include "config/topology-config-helper.h"
#include "config/cluster-config-vars.h"
#include "config/cluster-config-reader.h"
#include "config/physical-plan-helper.h"
#include "config/operational-config-vars.h"
#include "config/operational-config-reader.h"
#include "config/heron-internals-config-vars.h"
#include "config/heron-internals-config-reader.h"

#endif
