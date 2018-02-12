CREATE TABLE client.survey
(
    some_id bigserial,
    some_field character varying,
    PRIMARY KEY (some_id)
)
WITH (
    OIDS = FALSE
);

ALTER TABLE client.survey
    OWNER to andydixon;