CREATE OR REPLACE FUNCTION client.update_geom(m_name varchar(30), t_name varchar(30))
    RETURNS VOID AS
$func$
BEGIN

EXECUTE format
    (
    'ALTER TABLE client.%I
        ADD COLUMN geom geometry,
        ADD COLUMN geom_m geometry;

    UPDATE client.%I
        SET geom = geometry, geom_m = geom_measure
        FROM
            (SELECT
                geom as geometry,
                ST_AddMeasure(geom, dsegrefsd, dsegrefed) as geom_measure,
                sect_label
            FROM client.%I
            ) as hapms_transfer
        WHERE section_label = sect_label',
    m_name, m_name, t_name
    );
END
$func$ LANGUAGE plpgsql;