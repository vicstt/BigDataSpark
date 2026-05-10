FROM apache/spark:3.5.1

USER root

RUN apt-get update && \
    apt-get install -y curl python3-pip wget && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --no-cache-dir psycopg2-binary clickhouse-driver pandas numpy requests

RUN mkdir -p /opt/spark/jars

RUN wget -q -O /opt/spark/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    wget -q -O /opt/spark/jars/clickhouse-jdbc-0.4.6-all.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar && \
    chmod 644 /opt/spark/jars/*.jar

RUN mkdir -p /opt/app
WORKDIR /opt/app

RUN ls -la /opt/spark/jars/

CMD ["tail", "-f", "/dev/null"]