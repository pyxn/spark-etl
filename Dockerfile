# We're using Python image with Debian
FROM python:3.9-slim-buster

# Variable for Spark version to be installed
ENV SPARK_VERSION=3.1.2
ENV HADOOP_VERSION=3.2

# Install openjdk-11 (Java is needed for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and install Apache Spark to /opt/spark
RUN curl -sL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the requirements.txt file into our work directory
COPY requirements.txt .

# Upgrade pip and install any additional requirements
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of our application
COPY . .

# Our command will run the ETL script using PySpark
CMD ["spark-submit", "transform.py"]
