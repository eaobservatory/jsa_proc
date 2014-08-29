CREATE TABLE job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag VARCHAR(80) NOT NULL,
    state CHAR(1) NOT NULL DEFAULT "?",
    state_prev CHAR(1) NOT NULL DEFAULT "?",
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
    host VARCHAR(80) NOT NULL DEFAULT "unknown",
    FOREIGN KEY (job_id) REFERENCES job(id)
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE INDEX log_job_id ON log (job_id);


CREATE TABLE obs (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       job_id INTEGER NOT NULL,
       obsid VARCHAR(80) NOT NULL,
       obsidss VARCHAR(80) NOT NULL,

       utdate DATE NOT NULL,
       obsnum INTEGER NOT NULL,
       instrument VARCHAR(80) NOT NULL,
       backend VARCHAR(80) NOT NULL,
       subsys INTEGER NOT NULL,

       project VARCHAR(80),
       survey VARCHAR(80),
       scanmode VARCHAR(80),
       sourcename VARCHAR(80),
       obstype VARCHAR(80),

       association VARCHAR(80),

       FOREIGN KEY (job_id) REFERENCES job(id)
           ON DELETE RESTRICT ON UPDATE RESTRICT
);
CREATE INDEX obs_job_id ON obs (job_id);

CREATE TABLE tile (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       job_id INTEGER NOT NULL,
       tile INTEGER NOT NULL
);
CREATE INDEX tile_job_id ON tile (job_id);
CREATE INDEX tile_tile ON tile (tile);
