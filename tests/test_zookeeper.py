"""
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
import logging
import time

from nose.plugins.attrib import attr

from toolazydogs import zookeeper
from toolazydogs.zookeeper import  Watcher, EXCEPTIONS, SystemZookeeperError, DataInconsistency, RuntimeInconsistency, ConnectionLoss, MarshallingError, Unimplemented, OperationTimeout, BadArguments, APIError, NoNode, NoAuth, NoChildrenForEphemerals, BadVersion, NodeExists, NotEmpty, SessionExpired, InvalidCallback, InvalidACL, AuthFailed
from toolazydogs.zookeeper.zookeeper import _collect_hosts


def test_EXCEPTIONS():
    assert isinstance(EXCEPTIONS[-1](), SystemZookeeperError)
    assert isinstance(EXCEPTIONS[-2](), RuntimeInconsistency)
    assert isinstance(EXCEPTIONS[-3](), DataInconsistency)
    assert isinstance(EXCEPTIONS[-4](), ConnectionLoss)
    assert isinstance(EXCEPTIONS[-5](), MarshallingError)
    assert isinstance(EXCEPTIONS[-6](), Unimplemented)
    assert isinstance(EXCEPTIONS[-7](), OperationTimeout)
    assert isinstance(EXCEPTIONS[-8](), BadArguments)
    assert isinstance(EXCEPTIONS[-100](), APIError)
    assert isinstance(EXCEPTIONS[-101](), NoNode)
    assert isinstance(EXCEPTIONS[-102](), NoAuth)
    assert isinstance(EXCEPTIONS[-103](), BadVersion)
    assert isinstance(EXCEPTIONS[-108](), NoChildrenForEphemerals)
    assert isinstance(EXCEPTIONS[-110](), NodeExists)
    assert isinstance(EXCEPTIONS[-111](), NotEmpty)
    assert isinstance(EXCEPTIONS[-112](), SessionExpired)
    assert isinstance(EXCEPTIONS[-113](), InvalidCallback)
    assert isinstance(EXCEPTIONS[-114](), InvalidACL)
    assert isinstance(EXCEPTIONS[-115](), AuthFailed)

    try:
        EXCEPTIONS[666]()
        assert False, 'Non existent error code should have thrown an exception'
    except Exception:
        pass


def test_hosts():
    assert ([('a', 1)], '/abc') == _collect_hosts('a:1/abc')
    assert ([('a', 12913)], None) == _collect_hosts('a:12913')
    assert ([('a', 2181)], None) == _collect_hosts('a')
    assert ([('a', 2181)], '/abc') == _collect_hosts('a/abc')
    assert ([('a', 1), ('b', 2), ('c', 2181)], '/abc') == _collect_hosts('a:1,b:2,c/abc')


class Mine(Watcher):
    def __init__(self):
        pass

    def sessionConnected(self, session_id, session_password):
        print 'CONNECTED'

    def sessionExpired(self, session_id):
        print 'EXPIRED'


@attr('server')
def test_zookeeper():
    z = zookeeper.allocate('acabrera-001:12913,acabrera-002:12913,acabrera-003:12913/search', session_timeout=1.0)
    z.watchers.add(Mine())

    z.get_children('/')

    time.sleep(10)

    for child in z.get_children('/'):
        print child

    z.close()

    z = zookeeper.allocate('acabrera-001:12913,acabrera-002:12913,acabrera-003:12913/search')

    z.get_children('/')
    z.get_children('/')

    z.close()


def setup_module():
    logger = logging.getLogger('toolazydogs.zookeeper')

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(logging.Formatter('%(name)-12s[%(thread)d]: %(levelname)-8s %(message)s'))

    logger.addHandler(console)
