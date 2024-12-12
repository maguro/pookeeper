""" Pookeeper testing harnesses
 Copyright 2012 the original author or authors

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
"""
import atexit
from distutils.version import StrictVersion
from functools import wraps
import logging
import os
import unittest
import datetime
from nose import SkipTest

from pookeeper import DropableClient34
from pookeeper.common import ZookeeperCluster
from pookeeper import Watcher


LOGGER = logging.getLogger(__name__)

CLUSTER = None
DEBUG_LOG = False


def get_global_cluster():
    global CLUSTER
    if CLUSTER is None:
        ZK_HOME = os.environ.get("ZOOKEEPER_PATH")
        assert ZK_HOME, (
            "ZOOKEEPER_PATH environment variable must be defined.\n "
            "For deb package installations this is /usr/share/java")

        CLUSTER = ZookeeperCluster(ZK_HOME)
        atexit.register(lambda cluster: cluster.terminate(), CLUSTER)
    return CLUSTER


ZK_VERSION = StrictVersion(os.environ.get("ZOOKEEPER_VERSION"))

def zookeeper_version(minimum_version):
    def version_tester(method):
        @wraps(method)
        def new(self, *args, **kws):
            if ZK_VERSION < StrictVersion(minimum_version):
                raise SkipTest('Minimum Zookeeper version %s not met, found %s' % (minimum_version, ZK_VERSION))
            else:
                return method(self, *args, **kws)

        return new

    return version_tester


class ExpiredWatcher(Watcher):
    def __init__(self, lost):
        self.lost = lost

    def session_expired(self, session_id):
        self.lost.set()


class PookeeperTestHarness(object):
    """Harness for testing code that uses Pookeeper

    This object can be used directly or as a mixin. It supports starting
    and stopping a complete ZooKeeper cluster locally and provides an
    API for simulating errors and expiring sessions.

    Example::

        class MyTestCase(unittest.TestCase, PookeeperTestHarness):
            def setUp(self):
                self.setup_zookeeper()

                # additional test setup

            def tearDown(self):
                self.teardown_zookeeper()

            def test_something(self):
                something_that_needs_a_pookeeper_client(self.client)

            def test_something_else(self):
                something_that_needs_zk_servers(self.servers)

    """

    def __init__(self):
        self.client = None

    @property
    def cluster(self):
        return get_global_cluster()

    @property
    def servers(self):
        return ",".join([s.address for s in self.cluster])

    def _get_nonchroot_client(self):
        return DropableClient34(self.servers)

    def _get_client(self, **kwargs):
        return DropableClient34(self.hosts, **kwargs)


    def setup_zookeeper(self):
        """Create a ZK cluster and chrooted :class:`Client33`

        The cluster will only be created on the first invocation and won't be
        fully torn down until exit.
        """
        if not self.cluster[0].running:
            self.cluster.start()
        self.hosts = self.servers

    def teardown_zookeeper(self):
        """Clean up any ZNodes created during the test
        """
        if self.cluster[0].running:
            self.cluster.stop()


class PookeeperTestCase(unittest.TestCase, PookeeperTestHarness):
    def setUp(self):
        self.setup_zookeeper()
        if DEBUG_LOG:
            add_handler('pookeeper')
            add_handler('toolazydogs.pookeeper')

    def tearDown(self):
        self.teardown_zookeeper()


class DebugFormatter(logging.Formatter):
    converter = datetime.datetime.fromtimestamp

    def __init__(self, format, datefmt):
        logging.Formatter.__init__(self, format, datefmt)

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime('%I:%M:%S.%f')
            s = '%s.%03d' % (t, record.msecs)
        return s


def add_handler(name, log_level=logging.NOTSET, format='%(asctime)s %(name)-12s[%(threadName)s]: %(levelname)-8s %(message)s', datefmt='%I:%M:%S.%f'):
    logger = logging.getLogger(name)

    for handler in logger.handlers:
        logger.removeHandler(handler)

    console = logging.StreamHandler()
    console.setLevel(log_level)
    console.setFormatter(DebugFormatter(format, datefmt))

    logger.addHandler(console)

