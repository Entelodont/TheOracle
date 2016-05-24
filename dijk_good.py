def dijk(graph, startnode, endnode):
    # create dictionaries of distances as we go along and nodes that we need to go to
    left_nodes = graph.keys()
    dists = {}
    already_found_node_list = [] # initialize "optimal" nodes that have been found to empty
    before_nodes = {}

    for item in graph:
        dists[item] = 1000000 # make distance relatively large
        before_nodes[item] = None

    dists[startnode] = 0 # initialize distance to 0 since beginning here

    while len(already_found_node_list) < len(left_nodes):
        # create tmp list of nodes that have not been accessed yet
        keep_nodes_tmp = list()
        for item in left_nodes:
            if item not in already_found_node_list:
                keep_nodes_tmp.append(item)

        # create dictionary of these distances to these nodes
        keep_nodes = dict()
        for item in keep_nodes_tmp:
            keep_nodes[item] = dists[item]

        # find the shortest distance node
        best = min(keep_nodes, key = dists.get)
        already_found_node_list.append(best)

        # update distances accordingly, from 1000000 to whatever the node's weight is
        for item in graph[best]:
            if dists[item] > dists[best] + graph[best][item]:
                dists[item] = dists[best] + graph[best][item]
                before_nodes[item] = best

    return dists[endnode]

graph = {'0':{'1':2}, '1':{'0':2, '2':6}, '2':{'1':6}}
startnode = '0'
endnode = '2'
dijk(graph, startnode, endnode)