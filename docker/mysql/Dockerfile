FROM mysql:8.0

COPY docker/mysql/conf.d/binlog.cnf /etc/mysql/conf.d/binlog.cnf
COPY docker/mysql/init/ /docker-entrypoint-initdb.d/
