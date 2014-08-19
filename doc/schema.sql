CREATE TABLE job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag VARCHAR(80) NOT NULL,
    state CHAR(1) NOT NULL DEFAULT "?",
    state_prev CHAR(1) DEFAULT NULL,
    location VARCHAR(80) NOT NULL,
    foreign_id VARCHAR(80) DEFAULT NULL,
    mode VARCHAR(10) NOT NULL,
    parameters TEXT DEFAULT "",
    priority INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX job_state ON job (state);
CREATE UNIQUE INDEX job_tag ON job (tag);
CREATE INDEX job_location ON job (location);
CREATE INDEX job_foreign_id ON job (foreign_id);
CREATE UNIQUE INDEX job_location_id ON job (location, foreign_id);
CREATE INDEX job_priority ON job (priority);

CREATE TABLE input_file (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    filename VARCHAR(80) NOT NULL,
    FOREIGN KEY (job_id) REFERENCES job (id)
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE INDEX input_file_job_id ON input_file (job_id);
CREATE UNIQUE INDEX input_file_job_file ON input_file (job_id, filename);

CREATE TABLE output_file (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    filename VARCHAR(80) NOT NULL,
    FOREIGN KEY (job_id) REFERENCES job(id)
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE INDEX output_file_job_id ON output_file (job_id);
CREATE UNIQUE INDEX output_file_job_file ON output_file (job_id, filename);

CREATE TABLE log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    datetime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    state_prev CHAR(1) NOT NULL DEFAULT "?",
    state_new CHAR(1) NOT NULL DEFAULT "?",
    message TEXT NOT NULL DEFAULT "",
    FOREIGN KEY (job_id) REFERENCES job(id)
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE INDEX log_job_id ON log (job_id);
