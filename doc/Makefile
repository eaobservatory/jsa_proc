.PHONY: default all

default: all

all: schema-mysql.sql

%-mysql.sql: %.sql
	sed -e 's/AUTOINCREMENT/AUTO_INCREMENT/;s/^);/) ENGINE=InnoDB;/' $< > $@
