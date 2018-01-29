SELECT
  COALESCE
    ((simple.id || '.' || simple.path[1]::text)::float, simple.id) as id,
    simple.sect_label,
    simple.simple_geom as geom,
    ST_GeometryType(simple.simple_geom) as geom_type,
    ST_AsEWKT(simple.simple_geom) as geom_wkt
FROM (
  SELECT
    dumped.*,
    (dumped.geom_dump).geom as simple_geom,
    (dumped.geom_dump).path as path
  FROM (
    SELECT *, ST_Dump(geom) AS geom_dump FROM road_transport.hapms_network
  ) as dumped
) AS simple;

SELECT (road_transport.simple.sect_label)::text, count(*)
FROM road_transport.simple
GROUP BY road_transport.simple.sect_label
HAVING count(*) > 1