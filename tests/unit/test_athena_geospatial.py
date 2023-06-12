import awswrangler as wr
import shapely


def test_athena_geospatial(path, glue_table, glue_database):
    df = wr.athena.read_sql_query(
        "SELECT ST_Point(-121.7602, 46.8527) AS col_point, ST_Polygon('POLYGON EMPTY') AS col_polygon",
        database=glue_database,
        ctas_approach=False,
    )

    assert type(df["col_point"][0]) == shapely.geometry.point.Point
    assert type(df["col_polygon"][0]) == shapely.geometry.polygon.Polygon
