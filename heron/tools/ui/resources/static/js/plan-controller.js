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
 * Plan controller that holds the state of interaction between the logical and physical plan.
 *
 * There are two levels of highlight that can occur:
 * - When the user clicks on a logical or physical component, we 'focus' on that component
 *   and update the hash so that the page becomes bookmarkable. This also updates the detailed
 *   information in the table below.
 * - When a user hovers over a component that is in focus, we 'highlight' that component and
 *   dim all the others. This doesn't change any information in the details table and shouldn't
 *   result in any network requests.
 */
function PlanController(baseUrl, cluster, environ, toponame, physicalPlan, logicalPlan) {
  var manager = {};
  var d3instances, d3nodes, d3links, d3nodeText, d3containerCount, d3instanceCount;
  var instanceDetails = new StatTrendlines(baseUrl, cluster, environ, toponame, physicalPlan, logicalPlan);

  function getFocusedElement() {
    var parts = window.location.hash.substr(2).split('/');
    return {
      logicalComponent: parts[0],
      physicalComponent: parts[1]
    };
  }

  function updateContainerCount() {
    var containers = {}, instances = {};
    d3instances.each(function (d) {
      if (!d3.select(this).classed('fade')) {
        containers[d.stmgrId] = true;
        instances[d.id] = true;
      }
    });
    d3containerCount.text(_.size(containers));
    d3instanceCount.text(_.size(instances));
  }

  function highlightLogicalComponent(name) {
    d3instances.classed('fade', function (d) {
      return d.name !== name;
    });
    d3nodes.classed('fade', function (d) {
      return d.name !== name;
    });
    d3nodeText.classed('fade', function (d) {
      return d.name !== name;
    });
    d3links.classed('fade', true);
    updateContainerCount();
  }

  function highlightPhysicalComponent(name, instance) {
    d3instances.classed('fade', function (d) {
      return d.id !== instance;
    });
    d3nodes.classed('fade', function (d) {
      return d.name !== name;
    });
    d3nodeText.classed('fade', function (d) {
      return d.name !== name;
    });
    d3links.classed('fade', true);
    updateContainerCount();
  }

  function clearHighlight() {
    d3instances.classed('fade', false);
    d3nodes.classed('fade', false);
    d3links.classed('fade', false);
    d3nodeText.classed('fade', true);
    updateContainerCount();
  }

  function highlightFocusedElements() {
    d3instances = d3.selectAll('#physical-plan .instance');
    d3containerCount = d3.select('#container-count');
    d3instanceCount = d3.select('#instance-count');
    d3nodes = d3.selectAll('#logical-plan .node');
    d3links = d3.selectAll('#logical-plan .link');
    d3nodeText = d3.selectAll('#logical-plan text');
    var focused = getFocusedElement();
    if (!focused.logicalComponent) {
      clearHighlight();
    } else if (!focused.physicalComponent) {
      highlightLogicalComponent(focused.logicalComponent);
    } else {
      highlightPhysicalComponent(focused.logicalComponent, focused.physicalComponent);
    }
  }

  // remove the table at bottom of the page
  function clearFocus() {
    React.renderComponent(
      AllMetrics({
        baseUrl: baseUrl,
        cluster: cluster,
        environ: environ,
        topology: toponame
      }),
      document.getElementById('display-counters')
    );
    d3instances.each(function (d) {
      d.highlightable = true;
    });
    instanceDetails.showInstance('*', '*');
  }

  // re-render the table  for all instances of a logical component
  function focusOnLogicalComponent(name, id) {
    var isBolt = logicalPlan.bolts[name];
    var comp_type = isBolt ? 'bolt': 'spout';
    var comp_spout_type = isBolt ? undefined : logicalPlan.spouts[name]["spout_type"];
    var comp_spout_source = isBolt ? undefined : logicalPlan.spouts[name]["spout_source"]
    React.renderComponent(
      AllMetrics({
        baseUrl: baseUrl,
        cluster: cluster,
        environ: environ,
        topology: toponame,
        comp_type: comp_type,
        comp_name: name,
        comp_spout_type: comp_spout_type,
        comp_spout_source: comp_spout_source,
        instance: id,
        hoverOverInstance: function (d) { highlightPhysicalComponent(d.name, d.id); },
        hoverOutInstance: highlightFocusedElements
      }),
      document.getElementById('display-counters')
    );
    d3instances.each(function (d) {
      d.highlightable = d.name === name;
    });
    instanceDetails.showInstance(name, '*');
  }

  function focusOnPhysicalComponent(name, id) {
    focusOnLogicalComponent(name, id);
    d3instances.each(function (d) {
      d.highlightable = d.id === id;
    });
    instanceDetails.showInstance(name, id);
  }

  // update the table based on element that is currently in focus
  function updateFocusedElement() {
    var focused = getFocusedElement();
    if (!focused.logicalComponent) {
      clearFocus();
    } else if (focused.physicalComponent) {
      focusOnPhysicalComponent(focused.logicalComponent, focused.physicalComponent);
    } else {
      focusOnLogicalComponent(focused.logicalComponent);
    }
  }

  $(window).on('hashchange', updateFocusedElement);
  $(window).on('hashchange', highlightFocusedElements);

  // when the plan is initially drawn, update the highlighted elements and
  // also redraw the table
  manager.planDrawn = function () {
    updateFocusedElement();
  };

  // when the plan is resized, update the highlighted elements
  manager.planResized = function () {
    highlightFocusedElements();
  };

  // treat double click as intent to clear the focus
  $(document).on('dblclick', ".plans .graphics", function () {
    window.location.hash = "/";
  });
  $(document).on('click', ".reset", function () {
    window.location.hash = "/";
  });



  // only allow interaction with a logical component if no other logical
  // component is in focus
  manager.logicalComponentClicked = function (d) {
    window.location.hash = "/" + d.name;
    d3.event.stopPropagation();
  };

  manager.logicalComponentHoverOver = function (d) {
    if (!getFocusedElement().logicalComponent) {
      highlightLogicalComponent(d.name);
    }
  };

  manager.logicalComponentHoverOut = function (d) {
    if (!getFocusedElement().logicalComponent) {
      highlightFocusedElements();
    }
  };

  // only allow interaction with physical components that are in focus if a
  // physical component isn't already focused
  manager.physicalComponentClicked = function (d) {
    window.location.hash = '/' + d.name + '/' + d.id;
    d3.event.stopPropagation();
  };

  manager.physicalComponentHoverOver = function (d, tip) {
    var focused = getFocusedElement();
    if (d.highlightable) {
      highlightPhysicalComponent(d.name, d.id);
      tip.show(d);
    }
  };

  manager.physicalComponentHoverOut = function (d, tip) {
    var focused = getFocusedElement();
    if (d.highlightable) {
      highlightFocusedElements();
    }
    tip.hide(d);
  };


  return manager;
}
