CREATE OR REPLACE FUNCTION create_table_hapms(t_name varchar(30))
    RETURNS VOID AS
$func$
BEGIN

EXECUTE format('
    CREATE TABLE IF NOT EXISTS %I (
        section_label character varying(24) PRIMARY KEY,
        road_class integer,
        road_class_code character varying(12),
        road_class_name character varying(12),
        road_class_sort integer,
        road integer,
        road_number character varying(12),
        road_name character varying(72),
        road_sort bigint,
        section bigint,
        section_start_date date,
        section_end_date date,
        section_length numeric(12, 3),
        section_function integer,
        section_function_code character varying(24),
        section_function_name character varying(72),
        section_function_sort bigint,
        operational_area integer,
        operational_area_code character varying(12),
        operational_area_name character varying(48),
        operational_area_sort bigint,
        data_key integer,
        direction_key integer,
        direction_code character varying(12),
        direction_name character varying(48),
        direction_sort bigint,
        permanent_lanes integer,
        single_or_dual integer,
        single_or_dual_code character varying(24),
        single_or_dual_name character varying(72),
        single_or_dual_sort bigint,
        environment integer,
        environment_code character varying(12),
        environment_name character varying(48),
        environment_sort bigint,
        local_authority integer,
        local_authority_code character varying(24),
        local_authority_name character varying(72),
        local_authority_sort bigint,
        plan_reference character varying(48),
        start_chainage numeric(12, 3),
        end_chainage numeric(12, 3)
        )
    WITH (
        OIDS = FALSE
    );

    ALTER TABLE %I
        OWNER to andydixon;
    )', t_name);
END
$func$ LANGUAGE plpgsql;