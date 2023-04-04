#!/usr/bin/python3
import argparse
import logging
import multiprocessing
import queue
import time
# non std modules
import boto3
from botocore.exceptions import ClientError
# own modules
from SqliteSet import SqliteSet

logging.basicConfig(level=logging.INFO)

STATS = {
    "keys_total_source": 0,
    "keys_skipped_cached": 0,
    "keys_skipped_existing": 0,
    "keys_copied": 0,
    }

def reader(prefix=""):
    """
    reding from source and filling queue
    """
    # Create a reusable Paginator
    paginator = source_client.get_paginator('list_objects')
    # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate(Bucket=args.source_bucket, Prefix=prefix)
    for page in page_iterator:
        if not page.get("Contents"):  # if nothing is present
            break
        for key in page['Contents']:
            if key["Key"] in keycache:
                logging.info(f"object {key['Key']} already marked as copied, skipping")
                continue
            logging.info(f"putting {key['Key']} on queue")
            worklist.put(key["Key"])  # put on queue
    logging.warning(f"reader finished read {STATS['keys_total_source']} object keys from source")


def writer():
    """
    copy from client1/bucket_name1
    to client2/bucket_name2
    """
    while True:
        try:
            key = worklist.get()
        except queue.Empty:
            logging.info("worklist is empty, this writer will quit")
            break
        try:
            target_client.head_object(Bucket=target_bucket_name, Key=key)
            # found
            logging.info(f"skipping key {key} it already exists on target, use --overwrite to enable")
            keycache.add(key)
        except ClientError:
            # Not found
            logging.debug(f"reading object source/{source_bucket_name}/{key}")
            data = source_client.get_object(Bucket=source_bucket_name, Key=key)
            logging.debug(f"writing object target/{target_bucket_name}/{key}")
            res = target_client.upload_fileobj(data["Body"], target_bucket_name, key)
            logging.warning(f"copied {key} to target")
            logging.debug(res)
            keycache.add(key)
        finally:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Tool to copy Objects between different S3 Servers, from bucket to bucket")
    parser.add_argument("--source-endpoint-url", required=True, help="Source Endpoint URL")
    parser.add_argument("--source-access-key", required=True, help="Source Endpoint Access-Key")
    parser.add_argument("--source-secret", required=True, help="Source Endpoint Secret")
    parser.add_argument("--source-bucket", required=True, help="Source Endpoint Bucket")
    parser.add_argument("--target-endpoint-url", required=True, help="Target Endpoint URL")
    parser.add_argument("--target-access-key", required=True, help="Target Endpoint Access-Key")
    parser.add_argument("--target-secret", required=True, help="Target Endpoint Secret")
    parser.add_argument("--target-bucket", required=True, help="Target Endpoint Bucket")
    parser.add_argument("--prefixes", default="/", help="list of prefixes to split by reader processes, comma seperated")
    parser.add_argument("--enable-state", default=False, action="store_true", help="enable persistent state tracking of already copied objects")
    parser.add_argument("--state-file", default="state.db", help="name of State File of already copied objects")
    parser.add_argument("--threads", type=int, default=multiprocessing.cpu_count(), help="number of workers and readers")
    args = parser.parse_args()

    logging.info(f"connecting source s3 storage on {args.source_endpoint_url}")
    source_client = boto3.client(
        "s3",
        aws_access_key_id=args.source_access_key,
        aws_secret_access_key=args.source_secret,
        endpoint_url=args.source_endpoint_url
    )
    source_bucket_name = args.source_bucket

    logging.info(f"connecting target s3 storage on {args.target_endpoint_url}")
    target_client = boto3.client(
        "s3",
        aws_access_key_id=args.target_access_key,
        aws_secret_access_key=args.target_secret,
        endpoint_url=args.target_endpoint_url
    )
    target_bucket_name = args.target_bucket

    if args.enable_state:
        keycache = SqliteSet(args.state_file)  # to keep track of already copied objects
    else:
        keycache = set()  # only in memory tracking

    worklist = multiprocessing.Queue()  # global worklist

    # key collectors - aka readers
    prefixes = [item.strip() for item in args.prefixes.split(",")]  # split and remove spaces
    readers = []  # list of reader threads
    for prefix in prefixes:
        readers.append(multiprocessing.Process(target=reader, args=(prefix, ), daemon=True))

    for index, reader in enumerate(readers):
        logging.info(f"starting reader process number {index}")
        reader.start()
    logging.warning(f"started {len(readers)} reader processes/threads")

    # wait until anything is on worklist to start writer processes
    while worklist.empty():
        time.sleep(5)  # wait until something is on queue

    # starting writers
    writers = []
    for thread_num in range(args.threads):  # creating threads
        writers.append(multiprocessing.Process(target=writer, daemon=True))

    for index, thread in enumerate(writers):  # starting threads
        logging.info(f"starting writer process number {index}")
        thread.start()
    logging.warning(f"started {len(writers)} writer processes/threads")

    for thread in writers:  # wating for quit
        thread.join()

    logging.warning(f"finished")
