Stage I (see BS - Stage I PDF): PostgreSQL password for scripts must be available
as one of:
  - secrets/.psql.pass   (preferred; same name as course materials; not in git)
  - secrets/psql.pass    (alternate filename if you avoid dotfiles)
  - environment variable PGPASSWORD (export in shell before bash scripts/stage1.sh)

The file must contain exactly one line: the password, no extra spaces or blank lines.
If you see "fe_sendauth: no password supplied", the file is missing, empty, or wrong path.

On the IU Hadoop cluster, create the file once, for example:
  cd /path/to/bigdata-final-project
  echo -n 'YOUR_PASSWORD' > secrets/.psql.pass
  chmod 600 secrets/.psql.pass

PgAdmin (http://hadoop-01.uni.innopolis.ru/pgadmin): use your team EMAIL for login.
JDBC / psycopg2 / Sqoop: use team username (e.g. team35) and this password.

Never commit the real password file.
