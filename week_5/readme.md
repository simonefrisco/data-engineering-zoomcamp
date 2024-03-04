
# Week 5

# Video 1
## Processing Methods

### Batch processing (current week)

### Streaming processing (week 6)

# Week 5 Project

## Workflow

Lake CSV -> Python -> SQL (dbt) -> Spark -> Python

Advantages : 
- easy to manage
- retry
- scale

Disadvantages:

- delay

# Video 2
## Apache Spark

- Data Processing Engine
- Run in a distributed cluster
- Multi-language (Java/Scala)

## HIVE / Presto (Athena)

We can use it for SQL jobs (if you can express the job as plain SQL)

## Spark

For complex jobs and transformations

# Video 3

1) Build a basic ubuntu dockerfile in order to install the spark engine

cd spark-ubuntu
docker build . -t w5ubuntu
docker run -it --name w5spark w5ubuntu

apt-get update
apt-get install wget nano
wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz
export JAVA_HOME="jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"
java --version

wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
tar xzfv spark-3.3.2-bin-hadoop3.tgz
rm spark-3.3.2-bin-hadoop3.tgz
export SPARK_HOME="spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
spark-sheel

nano .bashrc

export JAVA_HOME="jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"
export SPARK_HOME="spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"

ctrl + o
ctrl + x

source .bashrc


2) enter in the spark instance

docker start -i w5spark
mkdir py-instance
cd py-istance

apt install python3.10-venv
python3 -m venv myenv
source myenv/bin/activate
pip install jupyter notebook




docker run  -it --name jnW5Spark -v $(pwd)/ddata:/home/ -p 8888:8888 jupyter/pyspark-notebook
docker start -i jnW5Spark

