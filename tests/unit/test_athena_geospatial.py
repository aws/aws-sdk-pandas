import geopandas
import pandas as pd
import shapely

import awswrangler as wr


def test_athena_geospatial(path, glue_table, glue_database):
    df = wr.athena.read_sql_query(
        """
        SELECT
            1 AS value
            , ST_Point(-121.7602, 46.8527) AS point
            , ST_LineFromText('LINESTRING(1 2, 3 4)') AS line
            , ST_Polygon('POLYGON ((1 1, 1 4, 4 4, 4 1))') AS polygon
            , ST_Polygon('POLYGON EMPTY') AS polygon_empty
        """,
        database=glue_database,
        ctas_approach=False,
    )

    assert type(df) == geopandas.GeoDataFrame

    assert type(df["value"]) == pd.Series
    assert type(df["point"]) == geopandas.GeoSeries
    assert type(df["line"]) == geopandas.GeoSeries
    assert type(df["polygon"]) == geopandas.GeoSeries
    assert type(df["polygon_empty"]) == geopandas.GeoSeries

    assert type(df["point"][0]) == shapely.geometry.point.Point
    assert type(df["line"][0]) == shapely.geometry.linestring.LineString
    assert type(df["polygon"][0]) == shapely.geometry.polygon.Polygon
    assert type(df["polygon_empty"][0]) == shapely.geometry.polygon.Polygon
