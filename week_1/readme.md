

# Video 1

## interactive mode
-it 

```
mkdir 2_docker_sql
cd 2_docker_sql

docker run hello-world

docker run -it ubuntu bash
```

remove everything

```
rm -rf /
rm -rf / --no-preserve-root
```

```
docker run -it python:3.9
```


```
docker run -it --entrypoint=bash python:3.9
pip install pandas
import pandas
pandas.__version__
```

with ctrl+D we can close the bash console

# start the docker file

with . docker build the image in this directory

```
docker build -t image_name:tag_name .
docker run -it image_name:tag_name

docker run -it image_name:tag_name all args that you want

```
pwd -> print current directory


# Video 2

download data 
create folder ./week_1/video_2/ny_taxi_postgres_data
```
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz 
```
```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v D:/obsidian/github/data-engineering-course/week_1/video_2/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

install pgcli and copy the data in the postgres
```
pip install pgcli
# for windows
pip install "psycopg[binary]"
```

connect to the db
```
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

print tables
```
/dt
```

run the notebook in order to upload the data

# Video 3

docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4