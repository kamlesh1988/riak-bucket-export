#!/usr/bin/env python

import sys
import urllib2
import urllib
import multiprocessing
import logging
import argparse
import urlparse
import json
import collections
import signal
import os
import email
from email.utils import parsedate
import tempfile

ChildEnv = collections.namedtuple(
    'ChildEnv', ['base_url', 'timeout', 'logger'])


def check_positive(value):
    ivalue = int(value)
    if ivalue < 0:
        raise argparse.ArgumentTypeError(
            "%s is an invalid positive int value" % value)
    return ivalue


def check_nonnegative(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError(
            "%s is an invalid non-negative int value" % value)
    return ivalue


def check_port(value):
    ivalue = int(value)
    if not (0 < ivalue < 65536):
        raise argparse.ArgumentTypeError(
            "%s is not a valid port number" % value)
    return ivalue


quote = urllib.quote_plus
unquote = urllib.unquote_plus


def parse_args():
    class LogLevelMap(dict):
        def __contains__(self, arg):
            return (super(LogLevelMap, self).__contains__(arg)
                    or arg in self.values())

    loglevels = LogLevelMap({
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warn': logging.WARN,
        'error': logging.ERROR,
        'fatal': logging.FATAL,
        'crit': logging.CRITICAL,
    })

    parser = argparse.ArgumentParser()
    parser.add_argument("bucket", help="name of bucket to dump")
    parser.add_argument("-H", "--host",
                        help="remote address. Default: localhost",
                        metavar="HOST",
                        default="localhost")
    parser.add_argument("-p", "--port",
                        help="remote port. Default: 8098",
                        type=check_port,
                        default=8098)
    parser.add_argument("-b", "--bucket-type",
                        help="type of bucket to dump. Default: no type",
                        default="default")
    parser.add_argument("-t", "--key-timeout",
                        help="timeout for key query. Default: 5",
                        type=check_positive, default=5)
    parser.add_argument("-T", "--list-timeout",
                        help="timeout for key listing. Default: 120",
                        type=check_positive,
                        default=120)
    parser.add_argument("-w", "--workers",
                        help="count of parallel workers used to extract keys. "
                        "Default: 20",
                        type=check_positive,
                        default=20)
    parser.add_argument("-B", "--batch-size",
                        help="batch size. Default: 100",
                        type=check_positive,
                        default=100)
    parser.add_argument("-C", "--tasks-per-child",
                        help="limit tasks per child to prevent memory leaks. "
                        "Default: None",
                        type=check_positive,
                        default=None)
    parser.add_argument("-o", "--output-file",
                        help="limit tasks per child to prevent memory leaks. "
                        "Default: BUCKET.{json,aof}")
    parser.add_argument("-l", "--loglevel",
                        help="logging verbosity. Default: warn",
                        type=lambda v: loglevels[v],
                        choices=loglevels,
                        default="warn")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--stream",
                       help="use streaming when keys are retrieved. Default",
                       action='store_true')
    group.add_argument("--no-stream",
                       help="do not use streaming when keys are retrieved.",
                       action='store_false')
    parser.add_argument("-F", "--format",
                        help="output format: json or Redis protocol. "
                        "Default: json",
                        choices=["json", "redis"],
                        default="json")
    parser.add_argument("-d", "--redis-db",
                        help="Redis database number. Default: 0",
                        type=check_nonnegative,
                        default=0)
    parser.add_argument("-P", "--redis-prefix",
                        help="prefix for all Redis keys. Default: \"\"",
                        default="")
    parser.add_argument("-a", "--redis-auth",
                        help="Redis password. Default: None",
                        default=None)
    args = parser.parse_args()
    return args


def setup_logger(loglevel):
    logger = multiprocessing.get_logger()
    logger.setLevel(loglevel)
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


def handle_result(key, value, ct):
    if ct == 'application/json':
        return key, json.dumps(json.loads(value)), ct
    elif ct == 'text/plain':
        return key, value, ct
    else:
        raise ValueError("Unknown content-type in response.")


def get_key(key, tries=10, attempt=0):
    if attempt >= 10:
        return key, None, None

    baseurl, timeout, logger = child_env
    url = baseurl + quote(key)

    try:
        resp = urllib2.urlopen(url, timeout=timeout)
        res = resp.read()
        ct = resp.info().getheader('Content-Type')
        res = handle_result(key, res, ct)

    except urllib2.HTTPError as e:
        if e.code == 300:
            try:
                req = urllib2.Request(url,
                                      headers={"Accept": "multipart/mixed"})
                resp = urllib2.urlopen(req, timeout=timeout)
                assert False, "Exception expected instead of OK response"
            except urllib2.HTTPError as e:
                if e.code == 300:
                    msgstr = "Content-Type: "
                    msgstr += e.info().getheader('Content-Type')
                    msgstr += "\r\n\r\n"
                    msgstr += e.read()
                    msg = email.message_from_string(msgstr)
                    last = max(msg.get_payload(),
                               key=lambda m: parsedate(m['last-modified']))

                    try:
                        res = handle_result(key, last.get_payload(),
                                            last['content-type'])
                    except Exception as e:
                        logger.warn("Unable to retrieve subkeys of key %s: %s",
                                    repr(key), str(e))

                elif e.code == 406:
                    return get_key(key, tries, attempt + 1)
                else:
                    logger.warn("Unable to retrieve subkeys of key %s: %s",
                                repr(key), str(e))
                    return key, None, None

        else:
            logger.warn("Unable to retrieve key %s: %s", repr(key), str(e))
            return key, None, None

    except Exception as e:
        logger.warn("Unable to retrieve key %s: %s", repr(key), str(e))
        return key, None, None
    return res


def sig_handler(signal, frame):
    sys.exit(0)


def make_bucket_url(bucket,
                    host="localhost", port=8098, bucket_type="default"):
    return urlparse.urlunparse(("http",
                                "%s:%d" % (host, port),
                                (("/types/" + quote(bucket_type)
                                  if bucket_type != "default"
                                  else "") +
                                 "/buckets/" + quote(bucket) + "/keys"),
                                "",
                                "",
                                ""))


def iterate_json_docs(docs):
    nesting = 0
    out = []
    for c in docs:
        out.append(c)
        if c == '{':
            nesting += 1
        elif c == '}':
            nesting -= 1
            assert nesting >= 0, "Unbalanced brackets while parsing JSON"
            if nesting == 0:
                yield "".join(out)
                del out
                out = []
    assert nesting == 0, "Unbalanced brackets while parsing JSON"


def bytes_from_file(f, chunksize=8192):
    while True:
        chunk = f.read(chunksize)
        if chunk:
            for b in chunk:
                yield b
        else:
            break


def retrieve_keylist(keys_url, logger, timeout=None, max_mem=2**20):
    stream = urllib2.urlopen(keys_url, timeout=timeout)
    out = tempfile.SpooledTemporaryFile(max_mem)
    count = 0
    for doc in iterate_json_docs(bytes_from_file(stream)):
        obj = json.loads(doc)
        for key in obj["keys"]:
            count += 1
            out.write(quote(key) + '\n')
    out.seek(0)
    logger.info("Retrieved key list from server. Parsing...")

    def feed_keys(f):
        for line in f:
            yield unquote(line).rstrip()
    return count, feed_keys(out)


class BaseRecordWriter(object):
    def __init__(self, output_file):
        raise NotImplementedError()

    def prolog(self):
        raise NotImplementedError()

    def epilog(self):
        raise NotImplementedError()

    def emit(self, key, value, content_type):
        raise NotImplementedError()


class JsonRecordWriter(BaseRecordWriter):
    def __init__(self, output_file):
        self.__out = output_file
        self.__need_comma = False

    def prolog(self):
        self.__out.write("{\n")

    def epilog(self):
        self.__out.write("\n}\n")

    def emit(self, key, value, content_type):
        if content_type == 'application/json':
            pass
        elif content_type == 'text/plain':
            value = json.dumps(value)
        else:
            raise ValueError('Unknown content type: %s' % repr(content_type))

        if self.__need_comma:
            self.__out.write(",\n" + json.dumps(key) + ": " + value)
        else:
            self.__out.write(json.dumps(key) + ":  " + value)
            self.__need_comma = True


class RedisRecordWriter(BaseRecordWriter):
    def __init__(self, output_file, db=0, prefix="", auth=None):
        self.__out = output_file
        self.__db = db
        self.__prefix = prefix
        self.__auth = auth
        self.__need_comma = False

    def gen_redis_proto(self, *args):
        proto = ''
        proto += '*' + str(len(args)) + '\r\n'
        for arg in args:
            arg = str(arg)
            proto += '$' + str(len(arg)) + '\r\n'
            proto += str(arg) + '\r\n'
        return proto

    def prolog(self):
        if self.__auth:
            self.__out.write(self.gen_redis_proto("AUTH", self.__auth))
        self.__out.write(self.gen_redis_proto("SELECT", self.__db))

    def epilog(self):
        pass

    def emit(self, key, value, content_type):
        self.__out.write(self.gen_redis_proto("SET",
                                              self.__prefix + key,
                                              value))


def main():
    signal.signal(signal.SIGINT, sig_handler)
    args = parse_args()
    out_filename = (args.output_file
                    if args.output_file
                    else (args.bucket + (".json"
                                         if args.format == "json"
                                         else ".aof")))
    out = open(out_filename, "w" if args.format == 'json' else "wb", 1)
    logger = setup_logger(args.loglevel)
    logger.info("stream=%s", args.no_stream)

    bucket_base_url = make_bucket_url(
        args.bucket, args.host, args.port, args.bucket_type)
    keys_url = bucket_base_url
    keys_url += "?keys="
    keys_url += "stream" if args.no_stream else "true"

    keys_count, keys = retrieve_keylist(
        keys_url, logger, args.list_timeout)
    logger.info("Loaded key list from bucket %s: items count = %d",
                (args.bucket_type, args.bucket), keys_count)

    env = ChildEnv(bucket_base_url + "/", args.key_timeout, logger)
    p = multiprocessing.Pool(
        args.workers, init_child, (env,), args.tasks_per_child)

    logger.info("Starting %d workers...", args.workers)
    writer = (JsonRecordWriter(out)
              if args.format == "json"
              else RedisRecordWriter(out,
                                     args.redis_db,
                                     args.redis_prefix,
                                     args.redis_auth))
    writer.prolog()
    counter = 0
    for key, value, ct in p.imap_unordered(get_key, keys, args.batch_size):
        if value:
            writer.emit(key, value, ct)
        counter += 1
        if counter % 10000 == 0:
            logger.info("Processed %d records (%.2f%% completed).",
                        counter, float(counter) / keys_count * 100)

    writer.epilog()
    logger.info("Finished.")


if __name__ == '__main__':
    main()
