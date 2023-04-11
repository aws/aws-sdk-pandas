"""Gremlin Init Module."""
# Required because `gremlin_python` does not initialize its modules in __init__.py

from awswrangler._utils import import_optional_dependency

if import_optional_dependency("gremlin_python"):
    from gremlin_python.driver.client import Client
    from gremlin_python.process.anonymous_traversal import traversal
    from gremlin_python.process.graph_traversal import GraphTraversalSource, __
    from gremlin_python.process.translator import Translator
    from gremlin_python.process.traversal import Cardinality, T
    from gremlin_python.structure.graph import Edge, Graph, Path, Property, Vertex, VertexProperty

    __all__ = [
        "__",
        "Cardinality",
        "Client",
        "Edge",
        "Graph",
        "GraphTraversalSource",
        "Path",
        "Property",
        "T",
        "Translator",
        "traversal",
        "Vertex",
        "VertexProperty",
    ]
