import igraph

def graph_ops(size):
    graph = igraph.Graph.Barabasi(size, 10)
    return graph.pagerank()[0]

def handler(event, context=None):
    size = 1000
    result = graph_ops(size)

    return {
        "result": "{} size graph BFS finished!".format(size)
    }


if __name__ == "__main__":
    event = {}
    print(handler(event))