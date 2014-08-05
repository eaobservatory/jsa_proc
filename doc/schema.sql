CREATE TABLE job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag VARCHAR(80),
    state VARCHAR(1)
);

CREATE INDEX job_state ON job (state);
CREATE INDEX job_tag ON job (tag);
