import heapq

class Graph:
    def __init__(self, vertices, neighbors):
        self.vertices = vertices
        self.neighbors = neighbors

    def __str__(self):
        return 'Graph(V={}, E={}'.format(self.vertices, self.neighbors)
        
def makeSpacesGraph(spaces):
    vertices = {s.id for s in spaces}
    neighbors = {space.id: set(space.neighbors) for space in spaces}
    return Graph(vertices, neighbors)


# TODO: cache this value so that we do not compute shortest path each time
def dijkstra(graph, srcVertex):
    D = {v : float('inf') for v in graph.vertices}
    P = {v : None for v in graph.vertices}
    D[srcVertex] = 0
    Q = {v for v in graph.vertices}
    while Q:
        u = min(Q, key=lambda x: D[x])
        Q.remove(u)
        if u not in graph.neighbors:
            continue
        for v in graph.neighbors[u]:
            if D[u] + 1 < D[v]:
                D[v] = D[u] + 1
                P[v] = u
    return D, P
                
        
def shortestPath(G, srcVertex, destVertex, D=None, P=None):
    if D is None and P is None:
        D, P = dijkstra(G, srcVertex)
    path = []
    while srcVertex != destVertex:
        path.append(destVertex)
        destVertex = P[destVertex]
        if destVertex is None: # This is the case where we have floor 2000, for example
            return None # We return None because there are no paths
    path.append(srcVertex)
    path.reverse()
    return path

def allPairsShortestPath(graph):
    allPairs = {}
    for start in graph.vertices:
        D, P = dijkstra(graph, start)
        for end in graph.vertices:
            path = shortestPath(graph, start, end, D, P)
            allPairs[(start, end)] = (0 if path is None else len(path), path)
    return allPairs
