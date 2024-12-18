FROM ubuntu:latest

RUN apt-get update && apt-get install -y \
    openjdk-11-jdk curl wget tar iputils-ping python3 python3-venv python3-pip

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3

RUN wget https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz \
    && tar -xzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -C /opt \
    && rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz \
    && ln -s /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV PYSPARK_PYTHON python3
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

COPY conf/spark-defaults.conf "$SPARK_HOME/conf"

RUN wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
RUN wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.771/aws-java-sdk-bundle-1.12.771.jar

ENV MINIO_VERSION=latest
RUN wget https://dl.min.io/server/minio/release/linux-amd64/minio \
    && chmod +x minio \
    && mv minio /usr/local/bin/

RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc \
    && chmod +x mc \
    && mv mc /usr/local/bin/

ENV MINIO_ROOT_USER=${MINIO_ROOT_USER}
ENV MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}
ENV MINIO_DATA_DIR=/data

COPY spark-minio-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/spark-minio-entrypoint.sh

RUN ln -s /usr/bin/python3 /usr/bin/python

COPY requirements/requirements.txt .
RUN python -m venv venv
RUN /bin/bash -c "source /venv/bin/activate"
RUN /bin/bash -c "source /venv/bin/activate && pip install -r requirements.txt"

CMD ["spark-minio-entrypoint.sh", "python3"]
