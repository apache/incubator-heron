################################################################################
# Adjacency list of edges in graph. This will correspond to the streams 
# in a topology DAG.
################################################################################
class Graph(object):
 
  def __init__(self):
    # graph adjecency list. Implemented as a map with key = vertices and 
    # values = set of Vertices in adjacency list.
    self.edges = {}
 
  def add_edge(self, U, V):
    if not U in self.edges:
      self.edges[U] = set()
    if not V in self.edges:
      self.edges[V] = set()
    if not V in self.edges[U]:
      self.edges[U].add(V)

  def __str__(self):
    print self.edges

  # Returns the maximum distance between any vertex and U in the connected 
  # component containing U
  def bfs_depth(self, U):
    bfs_queue = [[U, 0]]  # Stores the vertices whose BFS hadn't been completed.
    visited = set()
    max_depth = 0
    while bfs_queue:
      [V, depth] = bfs_queue.pop()
      if max_depth < depth:
        max_depth = depth
      visited.add(V)
      adj_set = self.edges[V]
      for W in adj_set:
        if W not in visited:
          bfs_queue.append([W, depth + 1])
    return max_depth

    
  # Returns the maximum distance between any vertex and U in the connected 
  # component containing U
  def diameter(self):
    diameter = 0
    for U in self.edges:
      depth = self.bfs_depth(U)
      if depth > diameter:
        diameter = depth
    return diameter

################################################################################
# Creates graph from logical plan, a deeply nested map structure that looks 
# like -
# {
#   'spouts' -> {
#      spout_name -> {
#        'outputs' -> ['stream_name' -> stream_name]
#      }
#   },
#   'bolts' -> {
#     bolt_name -> {
#       'outputs' -> ['stream_name'->sream_name],
#       'inputs' -> ['stream_name'->stream_name, 
#                      'component_name'->component_name,
#                      'grouping'->grouping_type]
#     }
#   }
# }
#
################################################################################
class TopologyDAG(Graph):
  def __init__(self, logical_plan):
    Graph.__init__(self)
    all_spouts = logical_plan['spouts']  # 'spouts' is required
    all_bolts = dict()
    if 'bolts' in logical_plan:
      all_bolts = logical_plan['bolts']
    stream_source = dict()  # Stores the mapping from input stream to component
    # Spout outputs
    for spout_name in all_spouts:
      for output_stream_data in all_spouts[spout_name]['outputs']:
        stream_name = output_stream_data['stream_name']
        stream_source[('%s,%s' % (spout_name,stream_name))] = spout_name
    # Bolt outputs
    for bolt_name in all_bolts:
      for output_stream_data in all_bolts[bolt_name]['outputs']:
        stream_name = output_stream_data['stream_name']
        stream_source[('%s,%s' % (bolt_name,stream_name))] = bolt_name

    # Add edges from stream_source to its destination. 
    for bolt_name in all_bolts:
      for input_stream_data in all_bolts[bolt_name]['inputs']:
        stream_name = input_stream_data['stream_name']
        component_name = input_stream_data['component_name']
        stream_hash = ('%s,%s' % (component_name,stream_name))
        self.add_edge(stream_source[stream_hash], bolt_name)

