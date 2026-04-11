Stage I (see BS - Stage I PDF): PostgreSQL password must be in a file named
  .psql.pass
in this directory (same as course materials; not committed to git).

On the IU Hadoop cluster, create it once, for example:
  echo -n 'YOUR_PASSWORD' > secrets/.psql.pass
  chmod 600 secrets/.psql.pass

PgAdmin (http://hadoop-01.uni.innopolis.ru/pgadmin): use your team EMAIL for login.
JDBC / psycopg2 / Sqoop: use team username (e.g. team35) and this password.

Copy psql.pass.example to .psql.pass locally only; never commit the real file.
