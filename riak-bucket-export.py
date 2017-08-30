#!/usr/bin/env python

import sys
import urllib2
import urllib
import multiprocessing
import json
import logging
import argparse
import urlparse
import collections
import signal
import os
import email
import email.utils

ChildEnv = collections.namedtuple('ChildEnv', ['base_url', 'timeout', 'logger'])

def check_positive(value):
    ivalue = int(value)
    if ivalue < 0:
         raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue

def check_port(value):
    ivalue = int(value)
    if not (0 < ivalue < 65536):
         raise argparse.ArgumentTypeError("%s is not a valid port number" % value)
    return ivalue

quote = urllib.quote_plus

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("bucket", help="name of bucket to dump")
    parser.add_argument("-H", "--host", help="remote address. Default: localhost", metavar="HOST", default="localhost")
    parser.add_argument("-p", "--port", help="remote port. Default: 8098", type=check_port, default=8098)
    parser.add_argument("-b", "--bucket-type", help="type of bucket to dump. Default: no type", default="default")
    parser.add_argument("-t", "--key-timeout", help="timeout for key query. Default: 5", type=check_positive, default=5)
    parser.add_argument("-T", "--list-timeout", help="timeout for key listing. Default: 120", type=check_positive, default=120)
    parser.add_argument("-w", "--workers", help="count of parallel workers used to extract keys. Default: 20", type=check_positive, default=20)
    parser.add_argument("-B", "--batch-size", help="batch size. Default: 100", type=check_positive, default=100)
    parser.add_argument("-C", "--tasks-per-child", help="limit tasks per child to prevent memory leaks. Default: None", type=check_positive, default=None)
    parser.add_argument("-o", "--output-file", help="limit tasks per child to prevent memory leaks. Default: BUCKET.json")
    args = parser.parse_args()
    return args

def setup_logger():
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.INFO)
    log_handler = logging.StreamHandler(sys.stderr)
    log_handler.setFormatter(
        logging.Formatter(
            '%(asctime)s %(levelname)-8s %(processName)s: %(message)s',
            '%Y-%m-%d %H:%M:%S'))
    logger.addHandler(log_handler)
    return logger

child_env = None

def init_child(env):
    global child_env
    child_env = env

def handle_result(res, ct):
    if ct == 'application/json':
        out = json.dumps(json.loads(res))
    elif ct == 'text/plain':
        out = json.dumps(res)
    else:
        raise ValueError("Unknown content-type in response.")
    return out

def get_key(key, tries = 10, attempt = 0):
    if attempt >= 10:
        return

    baseurl, timeout, logger = child_env
    url = baseurl + quote(key)

    try:
        resp = urllib2.urlopen(url, timeout=timeout)
        res = resp.read()
        ct = resp.info().getheader('Content-Type')
        res = handle_result(res, ct)

    except urllib2.HTTPError as e:
        if e.code == 300:
            try:
                req = urllib2.Request(url, headers={"Accept": "multipart/mixed"})
                resp = urllib2.urlopen(req, timeout=timeout)
                assert False, "Exception expected instead of OK response"
            except urllib2.HTTPError as e:
                if e.code == 300:
                    msgstr = "Content-Type: " + e.info().getheader('Content-Type') + "\r\n\r\n"
                    msgstr += e.read()
                    msg = email.message_from_string(msgstr)
                    last = max(msg.get_payload(),
                        key = lambda m: email.utils.parsedate(m['last-modified']))

                    try:
                        res = handle_result(last.get_payload(), last['content-type'])
                    except Exception as e:
                        logger.warn("Unable to retrieve subkeys of key %s: %s", repr(key), str(e))

                elif e.code == 406:
                    return get_key(key, tries, attempt + 1)
                else:
                    logger.warn("Unable to retrieve subkeys of key %s: %s", repr(key), str(e))
                    return

        else:
            logger.warn("Unable to retrieve key %s: %s", repr(key), str(e))
            return

    except Exception as e:
        logger.warn("Unable to retrieve key %s: %s", repr(key), str(e))
    return json.dumps(key) + ":  " + res

def sig_handler(signal, frame):
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, sig_handler)
    args = parse_args()
    out_filename = args.output_file if args.output_file else (args.bucket + ".json")
    out = open(out_filename, "w", 1)
    logger = setup_logger()

    keys_url = urlparse.urlunparse(
        ("http",
        "%s:%d" % (args.host, args.port),
        ("/types/" + quote(args.bucket_type) if args.bucket_type != "default" else "") + "/buckets/" + quote(args.bucket) + "/keys",
        "",
        "keys=true",
        ""))

    keys = json.load(urllib2.urlopen(keys_url, timeout = args.list_timeout))["keys"]
    keys_count = len(keys)
    logger.info("Loaded key list from bucket %s: items count = %d",
        (args.bucket_type, args.bucket), keys_count)

    bucket_base_url = urlparse.urlunparse(
        ("http",
        "%s:%d" % (args.host, args.port),
        ("/types/" + quote(args.bucket_type) if args.bucket_type != "default" else "") + "/buckets/" + quote(args.bucket) + "/keys/",
        "",
        "",
        ""))

    env = ChildEnv(bucket_base_url, args.key_timeout, logger)
    p = multiprocessing.Pool(args.workers, init_child, (env,), args.tasks_per_child)

    logger.info("Starting %d workers...", args.workers)
    out.write("{\n")
    last_res = None
    counter = 0
    for res in p.imap_unordered(get_key, keys, args.batch_size):
        if res is not None:
            if last_res is not None:
                out.write(last_res + ",\n")
            last_res = res
        counter += 1
        if counter % 10000 == 0:
            logger.info(
                "Processed %d records (%.2f%% completed).",
                counter,
                float(counter) / keys_count * 100)

    if last_res is not None:
        out.write(last_res + "\n")
    out.write("}\n")
    logger.info("Finished.")

if __name__ == '__main__':
    main()
