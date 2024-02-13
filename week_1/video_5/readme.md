

# init docker compose

with -d detach mode -> return the terminal
```
docker-compose up -d
```

python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"


# for windows

## 1) from python
pip install -r requirements.txt
python ingest_data.py    --user=root    --password=root    --host=localhost    --port=5432    --db=ny_taxi    --table_name=yellow_taxi_trips   --local_path="D:/obsidian/github/data-engineering-course/week_1/video_2/data/yellow_tripdata_2021-01.csv"

## 2) with docker

docker build -t taxi_ingest:v001 .
docker run -it \
    --network=pg-network \
    taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"


# Notes
## credit : https://github.com/ziritrion/dataeng-zoomcamp/blob/main/notes/1_intro.md

As a final note, SQL commands can be categorized into the following categories:

DDL: Data Definition Language.
Define the database schema (create, modify, destroy)
CREATE, DROP, ALTER, TRUNCATE, COMMENT, RENAME
DQL: Data Query Language.
Perform queries on the data within schema objects. Get data from the database and impose order upon it.
SELECT
DML: Data Manipulation Language.
Manipulates data present in the database.
INSERT, UPDATE, DELETE, LOCK...
DCL: Data Control Language.
Rights, permissions and other controls of the database system.
Usually grouped with DML commands.
GRANT, REVOKE
TCL: Transaction Control Language.
Transactions within the database.
Not a universally considered category.
COMMIT, ROLLBACK, SAVEPOINT, SET TRANSACTION