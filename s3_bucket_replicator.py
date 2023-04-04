#!/usr/bin/python3
import argparse
import logging
# non std modules
import boto3
from botocore.exceptions import ClientError
# own modules
from SqliteSet import SqliteSet

logging.basicConfig(level=logging.INFO)


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
                logging.debug("object {key['Key']} already marked as copied, skipping")
                continue
            logging.debug(f"reading object source/{source_bucket_name}/{key['Key']}")
            data = source_client.get_object(Bucket=source_bucket_name, Key=key["Key"])
            try:
                target_client.head_object(Bucket=target_bucket_name, Key=key["Key"])
                # found
                logging.info(f"skipping key {key['Key']} it already exists on target, use --overwrite to enable")
                keycache.add(key['Key'])
            except ClientError:
                # Not found
                logging.debug(f"writing object target/{target_bucket_name}/{key['Key']}")
                logging.info(f"copy {key['Key']} to target")
                res = target_client.upload_fileobj(data["Body"], target_bucket_name, key["Key"])
                logging.debug(res)
                keycache.add(key['Key'])
            finally:
                pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Tool so copy Objects between to S3 Servers")
    parser.add_argument("--source-endpoint-url", help="Source Endpoint URL")
    parser.add_argument("--source-endpoint-access-key", help="Source Endpoint Access-Key")
    parser.add_argument("--source-endpoint-secret", help="Source Endpoint Secret")
    parser.add_argument("--source-bucket", help="Source Endpoint Bucket")
    parser.add_argument("--target-endpoint-url", help="Target Endpoint URL")
    parser.add_argument("--target-endpoint-access-key", help="Target Endpoint Access-Key")
    parser.add_argument("--target-endpoint-secret", help="Target Endpoint Secret")
    parser.add_argument("--target-bucket", help="Target Endpoint Bucket")
    args = parser.parse_args()

    logging.info(f"connection source s3 storage on {args.source_endpoint_url}")
    source_client = boto3.client(
        "s3",
        aws_access_key_id=args.source_endpoint_access_key,
        aws_secret_access_key=args.source_endpoint_secret,
        endpoint_url=args.source_endpoint_url
    )
    source_bucket_name = args.source_bucket

    logging.info(f"connection target s3 storage on {args.target_endpoint_url}")
    target_client = boto3.client(
        "s3",
        aws_access_key_id=args.target_endpoint_access_key,
        aws_secret_access_key=args.target_endpoint_secret,
        endpoint_url=args.target_endpoint_url
    )
    target_bucket_name = args.target_bucket

    keycache = SqliteSet("./state.db")  # to keep track of already copied objects

    sync_storage(source_client, source_bucket_name, target_client, target_bucket_name)
