CREATE TABLE job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag VARCHAR(80) NOT NULL,
    state CHAR(1) NOT NULL DEFAULT "?",
    state_prev CHAR(1) DEFAULT NULL,
    location VARCHAR(80) NOT NULL,
    foreign_id VARCHAR(80) DEFAULT NULL,
    parameters TEXT DEFAULT ""
);

CREATE INDEX job_state ON job (state);
CREATE UNIQUE INDEX job_tag ON job (tag);
CREATE INDEX job_location ON job (location);
CREATE INDEX job_foreign_id ON job (foreign_id);
CREATE UNIQUE INDEX job_location_id ON job (location, foreign_id);

CREATE TABLE input_file (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES job(id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    filename VARCHAR(80) NOT NULL
);

CREATE INDEX input_file_job_id ON input_file (job_id);

CREATE TABLE output_file (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES job(id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    filename VARCHAR(80) NOT NULL
);

CREATE INDEX output_file_job_id ON output_file (job_id);

CREATE TABLE log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES job(id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    datetime INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP,
    state_prev CHAR(1) DEFAULT NULL,
    state_new CHAR(1) DEFAULT NULL,
    message TEXT NOT NULL DEFAULT ""
);

CREATE INDEX log_job_id ON log (job_id);
