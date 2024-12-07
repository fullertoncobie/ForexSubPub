"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Kevin Lundeen
:Version: f19-02

Implementation of Bellman-Ford Algorithm for Lab 3.
"""


class BellmanFord(object):
    """
    Graph suitable for Bellman-Ford Algorithm. Edges are added with the
    add_edge method. Shortest paths (and cycles)
    can then be determined with the shortest_paths method.
    """

    def __init__(self, initial_edges=None):
        self.vertices = set()
        self.edges = {}
        if initial_edges is not None:
            for u in initial_edges:
                for v in initial_edges[u]:
                    self.add_edge(u, v, initial_edges[u][v])

    def add_edge(self, from_vertex, to_vertex, weight):
        """
        Add an edge (and possibly the from and to vertices) to this graph.
        If the edge already exists, the weight is changed to the given one.

        :param from_vertex: start of edge
        :param to_vertex: end of edge
        :param weight: weight of edge
        """
        if from_vertex == to_vertex:
            raise ValueError(
                '{} -> {}: {}'.format(from_vertex, to_vertex, weight))
        self.vertices.add(from_vertex)
        self.vertices.add(to_vertex)
        if from_vertex not in self.edges:
            self.edges[from_vertex] = {}
        self.edges[from_vertex][to_vertex] = weight

    def remove_edge(self, from_vertex, to_vertex):
        try:
            del self.edges[from_vertex][to_vertex]
        except KeyError:
            raise KeyError('remove_edge({}, {})'.format(from_vertex, to_vertex))

    def shortest_paths(self, start_vertex, tolerance=0):
        """
        Find the shortest paths (sum of edge weights) from start_vertex to
        every other vertex. Also detect if there are negative cycles and
        report one of them. Edges may be negative.

        For relaxation and cycle detection, we use tolerance. Only
        relaxations resulting in an improvement greater than tolerance are
        considered. For negative cycle detection, if the sum of weights is
        greater than -tolerance it is not reported as a negative cycle. This
        is useful when circuits are expected to be close to zero.

        >>> g = BellmanFord({'a': {'b': 1, 'c':5}, 'b': {'c': 2, 'a': 10}, 'c': {'a': 14, 'd': -3}, 'e': {'a': 100}})
        >>> dist, prev, neg_edge = g.shortest_paths('a')
        >>> [(v, dist[v]) for v in sorted(dist)]  # shortest distance from 'a' to each other vertex
        [('a', 0), ('b', 1), ('c', 3), ('d', 0), ('e', inf)]
        >>> [(v, prev[v]) for v in sorted(prev)]  # last edge in shortest paths
        [('a', None), ('b', 'a'), ('c', 'b'), ('d', 'c'), ('e', None)]
        >>> neg_edge is None
        True
        >>> g.add_edge('a', 'e', -200)
        >>> dist, prev, neg_edge = g.shortest_paths('a')
        >>> neg_edge  # edge where we noticed a negative cycle
        ('e', 'a')

        :param start_vertex: start of all paths
        :param tolerance: only if a path is more than tolerance better will
                          it be relaxed
        :return: (distance, predecessor, negative_cycle)
            distance:       dictionary keyed by vertex of shortest distance
                            from start_vertex to that vertex
            predecessor:    dictionary keyed by vertex of previous vertex in
                            shortest path from start_vertex
            negative_cycle: None if no negative cycle, otherwise an edge,
                            (u,v), in one such cycle
        """
        # initialize
        distance, predecessor = {}, {}
        for v in self.vertices:
            distance[v] = float('inf')
            predecessor[v] = None
        distance[start_vertex] = 0

        # repeated relaxation
        for i in range(len(self.vertices)):
            for u in self.edges:
                for v in self.edges[u]:
                    w = self.edges[u][v]
                    if distance[v] - (distance[u] + w) > tolerance:
                        if v == start_vertex:
                            return distance, predecessor, (u, v)
                        distance[v] = distance[u] + w
                        predecessor[v] = u

        # check for negative cycles
        negative_cycle = None
        for u in self.edges:
            for v in self.edges[u]:
                w = self.edges[u][v]
                if distance[v] - (distance[u] + w) > tolerance:
                    return distance, predecessor, (u, v)

        return distance, predecessor, None
