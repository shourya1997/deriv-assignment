-- G1 fix — sql/01-04 reference raw/staging/quarantine/data_quality/warehouse but
-- never create the schemas themselves (sql/01 even skips `CREATE SCHEMA warehouse`).
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS quarantine;
CREATE SCHEMA IF NOT EXISTS data_quality;
