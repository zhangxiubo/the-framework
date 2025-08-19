import networkx as nx

from framework.core import Pipeline
from framework.reactive import Collector, ReactiveBuilder, ReactiveEvent, reactive


def test_bfs():
    g = nx.random_graphs.gnp_random_graph(n=10, p=0.3, seed=123, directed=True)

    @reactive("n", ["n"])
    class BFSWalker(ReactiveBuilder):
        def build(self, context, target, node, *args, **kwargs):
            print("visiting", node)
            for n in g.neighbors(node):
                yield n

    print(list(nx.bfs_tree(g, source=0)))

    pipeline = Pipeline([BFSWalker(), collector := Collector("ns", ["n"])])
    pipeline.submit(
        ReactiveEvent(name="built", target="n", artifact=list(g.nodes)[0])
    )  # kicks of the resolution with a source node
    pipeline.submit(ReactiveEvent(name="resolve", target="ns"))
    pipeline.run()

    assert [n for n, *_ in collector.values] == list(nx.bfs_tree(g, source=0))
