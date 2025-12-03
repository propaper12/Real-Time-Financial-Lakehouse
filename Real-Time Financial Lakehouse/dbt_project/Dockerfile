FROM python:3.9-slim

RUN pip install dbt-duckdb s3fs

WORKDIR /usr/app

COPY . .
CMD ["tail", "-f", "/dev/null"]