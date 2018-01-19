CREATE DATABASE ins_data_dev
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

COMMENT ON DATABASE ins_data_dev
    IS 'Development Database for the INS Data Team processing development';
