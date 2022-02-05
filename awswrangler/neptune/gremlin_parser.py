from typing import Dict, Any

from gremlin_python.structure.graph import Path
from gremlin_python.structure.graph import Vertex
from gremlin_python.structure.graph import Edge
from gremlin_python.structure.graph import VertexProperty
from gremlin_python.structure.graph import Property


class GremlinParser:
    @staticmethod
    def gremlin_results_to_dict(result) -> Dict[str, Any]:
        res = []

        # For lists or paths unwind them
        if isinstance(result, list) or isinstance(result, Path):
            for x in result:
                res.append(GremlinParser._parse_dict(x))

        # For dictionaries just add them
        elif isinstance(result, dict):
            res.append(result)

        # For everything else parse them
        else:
            res.append(GremlinParser._parse_dict(result))
        return res

    @staticmethod
    def _parse_dict(data) -> Dict[str, Any]:
        d = dict()

        # If this is a list or Path then unwind it
        if isinstance(data, list) or isinstance(data, Path):
            res = []
            for x in data:
                res.append(GremlinParser._parse_dict(x))
            return res

        # If this is an element then make it a dictionary
        elif isinstance(data, Vertex) or isinstance(data, Edge) or isinstance(data, VertexProperty) or isinstance(data,
                                                                                                                  Property):
            data = data.__dict__

        # If this is a scalar then create a Map with it
        elif not hasattr(data, "__len__") or isinstance(data, str):
            data = {0: data}

        for (k, v) in data.items():
            # If the key is a Vertex or an Edge do special processing
            if isinstance(k, Vertex) or isinstance(k, Edge):
                k = k.id

            # If the value is a list do special processing to make it a scalar if the list is of length 1
            if isinstance(v, list) and len(v) == 1:
                d[k] = v[0]
            else:
                d[k] = v

            # If the value is a Vertex or Edge do special processing
            if isinstance(d[k], Vertex) or isinstance(d[k], Edge) or isinstance(d[k], VertexProperty) or isinstance(
                    d[k], Property):
                d[k] = d[k].__dict__
        return d
