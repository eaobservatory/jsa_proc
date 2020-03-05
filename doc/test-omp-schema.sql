CREATE TABLE ompobslog (
  obslogid INTEGER PRIMARY KEY AUTOINCREMENT,
  runnr int(11) NOT NULL,
  instrument varchar(32) NOT NULL,
  telescope varchar(32) DEFAULT NULL,
  date datetime NOT NULL,
  obsactive int(11) NOT NULL,
  commentdate datetime NOT NULL,
  commentauthor varchar(32) NOT NULL,
  commenttext longtext DEFAULT NULL,
  commentstatus int(11) NOT NULL,
  obsid varchar(48) DEFAULT NULL
);
