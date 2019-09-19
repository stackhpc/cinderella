#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

import argparse
import os
import sys
import logging
import json
from datetime import datetime, timezone, timedelta
import requests
from requests import Request, Session

import jmespath
import pytimeparse

from prometheus_http_client.prometheus import relabel

from cinderella import __version__

__author__ = "Will Szumski"
__copyright__ = "Will Szumski"
__license__ = "apache"

_logger = logging.getLogger(__name__)


def hottest_metrics(top):
    @relabel('topk(%s, count({__name__=~".+"}) by (__name__))' % top)
    def _hottest_metrics(self, **kwargs):
        pass

    return _hottest_metrics()


@relabel('count({__name__=~".+"}) by (__name__)')
def all_metrics():
    pass


url = os.getenv('PROMETHEUS_URL', 'http://localhost:9090')
headers = os.getenv('PROMETHEUS_HEAD', None)
if headers:
    headers = json.loads(headers)


def delete(metric, end):
    _logger.info("deleting: {}".format(metric))
    # TODO: check status field
    s = Session()
    req = Request(
        'POST', url.rstrip('/') + '/api/v1/admin/tsdb/delete_series',
        params={
            'match[]': '%s' % metric,
            'end': str((int(end)))
        }, headers=headers
    )
    prepped = req.prepare()
    _logger.info("request url: {}".format(prepped.url))
    result = s.send(prepped)
    if result.status_code != 204:
        _logger.error(result.text)
        raise ValueError("There was an error deleting: %s" % metric)


def clean_tombstones():
    result = requests.post(
        url=url.rstrip('/') + '/api/v1/admin/tsdb/clean_tombstones',
        headers=headers)
    if result.status_code != 204:
        _logger.error(result.text)
        raise ValueError("clean tombstones did not return 204")


class CinderellaCLI(object):
    def __init__(self):
        parser = argparse.ArgumentParser(
            description="Prometheus clean up tool")
        parser.add_argument('command', help='Subcommand to run', choices=[
            'top', 'delete', 'list'
        ])
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('Unrecognized command')
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        getattr(self, args.command)()

    @staticmethod
    def _add_common(parser):
        parser.add_argument(
            '--version',
            action='version',
            version='cinderella {ver}'.format(ver=__version__))
        parser.add_argument(
            '-v',
            '--verbose',
            dest="loglevel",
            help="set loglevel to INFO",
            action='store_const',
            const=logging.INFO)
        parser.add_argument(
            '-vv',
            '--very-verbose',
            dest="loglevel",
            help="set loglevel to DEBUG",
            action='store_const',
            const=logging.DEBUG)

    def top(self):
        parser = argparse.ArgumentParser(
            description='list time series with the most samples')
        parser.add_argument(
            dest="n",
            help="list time series with the most samples",
            type=int,
            metavar="INT")
        CinderellaCLI._add_common(parser)
        args = parser.parse_args(sys.argv[2:])
        setup_logging(args.loglevel)
        top_by_samples(args.n)

    def list(self):
        parser = argparse.ArgumentParser(
            description='list time all metrics')
        CinderellaCLI._add_common(parser)
        args = parser.parse_args(sys.argv[2:])
        setup_logging(args.loglevel)
        list_metrics()

    def delete(self):
        parser = argparse.ArgumentParser(
            description='list time series with the most samples')
        parser.add_argument(
            dest="metric",
            help="metric to delete",
            metavar="METRIC")
        parser.add_argument(
            dest="time_range",
            help="timerange to preserve",
            metavar="TIMERANGE")
        CinderellaCLI._add_common(parser)
        args = parser.parse_args(sys.argv[2:])
        setup_logging(args.loglevel)
        seconds = pytimeparse.parse(args.time_range)
        delta = timedelta(seconds=seconds)
        now = datetime.now()
        then = now - delta
        end = then.replace(tzinfo=timezone.utc).timestamp()
        delete(args.metric, end)
        _logger.info(clean_tombstones())


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(level=loglevel, stream=sys.stdout,
                        format=logformat, datefmt="%Y-%m-%d %H:%M:%S")


def list_metrics():
    query_result = json.loads(all_metrics())
    success = query_result["status"] == "success"
    if not success:
        raise ValueError("hottest metrics query failed")
    metrics = jmespath.search("data.result[]", query_result)
    result = []
    for metric in metrics:
        meta = metric["metric"]
        result.append(meta["__name__"])
    for metric in sorted(result):
        print(metric)


def top_by_samples(n):
    topk = json.loads(hottest_metrics(n))
    success = topk["status"] == "success"
    if not success:
        raise ValueError("hottest metrics query failed")
    metrics = jmespath.search("data.result[]", topk)
    result = []
    for metric in metrics:
        meta = metric["metric"]
        out = {
            "name": meta["__name__"],
            # timestamp is in zeroth position
            "value": metric["value"][1]
        }
        result.append(out)
    print(json.dumps(result, sort_keys=True, indent=4))


def run():
    """Entry point for console_scripts
    """
    CinderellaCLI()


if __name__ == "__main__":
    run()
