# MetaHive

We promise you a pony.


## Requisites:
apt install python-flask
pip install htsql
pip install HTSQL-MYSQL


## API:
for now, spawn via CLI and setup a reverse proxy for the api... URL.
python /usr/src/metahive/api/hive-api.py

## Config: /etc/metahive/metahive.conf
```
[database]
db_username = username
db_password = password
db_name = name_of_database

[repository]
directory = /path/to/the/hashed_repository/
```
