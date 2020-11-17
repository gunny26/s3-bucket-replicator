FROM python:3.8-slim
MAINTAINER Arthur Messner <arthur.messner@gmail.com>
WORKDIR /usr/src/app
# some requirement to install
COPY requirements.txt ./
# needed, TLS Scanner
# COPY CA.pem ./
# mind the proxy and CA setting
# RUN pip install --proxy=<some-proxy> --cert CA.pem --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
# directory to store data, should be persistent
# RUN mkdir -p /usr/src/app/hot
# also config file MUST be volume, there is no default provided
RUN mkdir /usr/src/app/db
COPY ./SqliteSet.py .
COPY ./s3_bucket_replicator.py .
COPY ./default_config.yml .
# -u is needed to see /dev/stdout immediately
CMD [ "python", "-u", "s3_bucket_replicator.py", "config.yml"]

