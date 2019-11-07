/*
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

/**
 * Navigator of the topology details section in the topology page.
 * When a tab is clicked, the corresponding "display-" class
 * is set to the "topologydetails" element. Then CSS shows/hides
 * the information.
 */
(function () {
  var selected = "stats";  // Show stats tab at beginning.

  function navigate () {
    this.parentElement.selected=this.id;
    for (var i = 0; i < this.parentElement.children.length; ++i) {
      this.parentElement.children[i].className = '';  // Hide all
    }
    this.className = 'active';  // Show "this" element.

    // Show the right div by setting the class of the outter div.
    d3.selectAll('div#topologydetails').attr('class', 'display-' + this.id);
  }

  d3.selectAll('.navigator button').on('click', navigate);
}());
