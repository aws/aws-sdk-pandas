# mypy: disable-error-code=name-defined
"""Amazon Neptune GremlinParser Module (PRIVATE)."""

from __future__ import annotations

from typing import Any

import awswrangler.neptune._gremlin_init as gremlin


class GremlinParser:
    """Class representing a parser for returning Gremlin results as a dictionary."""

    @staticmethod
    def gremlin_results_to_dict(result: Any) -> list[dict[str, Any]]:
        """Take a Gremlin ResultSet and return a dictionary.

        Parameters
        ----------
        result : Any
            The Gremlin result set to convert

        Returns
        -------
        List[Dict[str, Any]]
            A list of dictionary results
        """
        res = []

        # For lists or paths unwind them
        if isinstance(result, (list, gremlin.Path)):
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
    def _parse_dict(data: Any) -> Any:
        d: dict[str, Any] = {}

        # If this is a list or Path then unwind it
        if isinstance(data, (list, gremlin.Path)):
            res = []
            for x in data:
                res.append(GremlinParser._parse_dict(x))
            return res

        # If this is an element then make it a dictionary
        if isinstance(
            data,
            (
                gremlin.Vertex,
                gremlin.Edge,
                gremlin.VertexProperty,
                gremlin.Property,
            ),
        ):
            data = data.__dict__

        # If this is a scalar then create a Map with it
        elif not hasattr(data, "__len__") or isinstance(data, str):
            data = {0: data}

        for k, v in data.items():
            # If the key is a Vertex or an Edge do special processing
            if isinstance(k, (gremlin.Vertex, gremlin.Edge)):
                k = k.id  # noqa: PLW2901

            # If the value is a list do special processing to make it a scalar if the list is of length 1
            if isinstance(v, list) and len(v) == 1:
                d[k] = v[0]
            else:
                d[k] = v

            # If the value is a Vertex or Edge do special processing
            if isinstance(
                data,
                (
                    gremlin.Vertex,
                    gremlin.Edge,
                    gremlin.VertexProperty,
                    gremlin.Property,
                ),
            ):
                d[k] = d[k].__dict__
        return d
