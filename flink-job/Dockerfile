FROM flink:1.18-scala_2.12

# Install Python and PyFlink
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install python dependencies
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Download Kafka Connector JAR (Essential for Flink-Kafka communication)
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.0.1-1.18/flink-connector-kafka-3.0.1-1.18.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

WORKDIR /opt/flink
COPY feature_engine.py .
