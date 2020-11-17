#!/usr/bin/python3
# taken from https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html
# modified to use custom S3 configuration
import os
import sys
import logging
# non std modules
import boto3
from botocore.exceptions import ClientError
import yaml
# own modules
from SqliteSet import SqliteSet

def read_config(filename):
    # read global config
    config = yaml.safe_load(open(filename, "rt"))
    logging.debug(config)
    return config

def sync_storage(source_client, source_bucket_name, target_client, target_bucket_name):
    """
    copy from client1/bucket_name1
    to client2/bucket_name2
    """
    # Create a reusable Paginator
    paginator = source_client.get_paginator('list_objects')
    # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate(Bucket=source_bucket_name)
    for page in page_iterator:
        for key in page['Contents']:
            if key["Key"] in keycache:
                continue
            print(f"reading object source/{source_bucket_name}/{key['Key']}")
            data = source_client.get_object(Bucket=source_bucket_name, Key=key["Key"])
            try:
                target_client.head_object(Bucket=target_bucket_name, Key=key["Key"])
                # found
                print(f"skipping key {key['Key']} it already exists")
                keycache.add(key['Key'])
            except ClientError:
                # Not found
                print(f"writing object target/{target_bucket_name}/{key['Key']}")
                print(f"copy {key['Key']} to target")
                res = target_client.upload_fileobj(data["Body"], target_bucket_name, key["Key"])
                keycache.add(key['Key'])
            finally:
                pass

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("usage <config_filename>")
        sys.exit(1)
    if not os.path.isfile(sys.argv[1]):
        print(f"file {sys.argv[1]} does not exist")
        sys.exit(2)
    config = read_config(sys.argv[1])
    source_client = boto3.client(
        "s3",
        aws_access_key_id=config["source"]["aws_access_key_id"],
        aws_secret_access_key=config["source"]["aws_secret_access_key"],
        endpoint_url=config["source"]["endpoint_url"]
    )
    source_bucket_name = config["source"]["bucket_name"]
    target_client = boto3.client(
        "s3",
        aws_access_key_id=config["target"]["aws_access_key_id"],
        aws_secret_access_key=config["target"]["aws_secret_access_key"],
        endpoint_url=config["target"]["endpoint_url"]
    )
    target_bucket_name = config["target"]["bucket_name"]
    keycache = SqliteSet(f"db/{config['source']['bucket_name']}_state.db")
    sync_storage(source_client, source_bucket_name, target_client, target_bucket_name)
