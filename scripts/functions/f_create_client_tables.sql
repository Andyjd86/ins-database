CREATE OR REPLACE FUNCTION client.create_client_tables()
    RETURNS VOID AS
$func$
BEGIN

EXECUTE format('
    CREATE TABLE client.path_master
        (
        path_id bigserial,
        fid_trail bigint[],
        fid_last bigint,
        geom geometry,
        new_path boolean DEFAULT true
        )
    WITH
        (
        OIDS = FALSE
        );

    ALTER TABLE client.path_master
        OWNER to andydixon;

    CREATE TABLE client.project_path
        (
        pk_id bigserial,
        path_id bigint,
        fid bigint,
        path_order bigint
        )
    WITH
        (
        OIDS = FALSE
        );

    ALTER TABLE client.project_path
        OWNER to andydixon;');
END
$func$ LANGUAGE plpgsql;