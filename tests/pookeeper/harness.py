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
import logging
import os
import unittest

from pookeeper import DropableClient34
from pookeeper.common import ZookeeperCluster
from toolazydogs.pookeeper import  Watcher


LOGGER = logging.getLogger(__name__)

CLUSTER = None


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

    def tearDown(self):
        self.teardown_zookeeper()


def add_handler(name, log_level=logging.NOTSET, format='%(name)-12s[%(thread)d]: %(levelname)-8s %(message)s'):
    logger = logging.getLogger(name)

    for handler in logger.handlers:
        logger.removeHandler(handler)

    console = logging.StreamHandler()
    console.setLevel(log_level)
    console.setFormatter(logging.Formatter(format))

    logger.addHandler(console)

