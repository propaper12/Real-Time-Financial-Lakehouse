FROM python:3.9-bullseye

RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

RUN pip install pyspark==3.4.1 numpy pandas delta-spark==2.4.0 deltalake

WORKDIR /app
COPY consumer_lake.py .
COPY process_silver.py .
COPY train_model.py .

CMD ["python", "consumer_lake.py"]