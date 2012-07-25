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
from toolazydogs.zookeeper import CREATE_CODES, Persistent, Ephemeral, PersistentSequential, EXCEPTIONS, APIError, InvalidACLError, AuthFailedError, InvalidCallbackError, SessionExpiredError, NotEmptyError, NodeExistsError, NoChildrenForEphemeralsError, BadVersionError, NoAuthError, NoNodeError, BadArgumentsError, OperationTimeoutError, UnimplementedError, MarshallingError, ConnectionLoss, DataInconsistency, RuntimeInconsistency, SystemZookeeperError, EphemeralSequential, Watcher, CREATOR_ALL_ACL, READ_ACL_UNSAFE
from toolazydogs.zookeeper.hosts import collect_hosts
from toolazydogs.zookeeper.zookeeper import _prefix_root, _hex


def test_CREATE_CODES():
    assert isinstance(CREATE_CODES[0], Persistent)
    assert CREATE_CODES[0].flags == 0
    assert CREATE_CODES[0].ephemeral == False
    assert CREATE_CODES[0].sequential == False
    assert str(CREATE_CODES[0]) == 'PERSISTENT'

    assert isinstance(CREATE_CODES[1], Ephemeral)
    assert CREATE_CODES[1].flags == 1
    assert CREATE_CODES[1].ephemeral == True
    assert CREATE_CODES[1].sequential == False
    assert str(CREATE_CODES[1]) == 'EPHEMERAL'

    assert isinstance(CREATE_CODES[2], PersistentSequential)
    assert CREATE_CODES[2].flags == 2
    assert CREATE_CODES[2].ephemeral == False
    assert CREATE_CODES[2].sequential == True
    assert str(CREATE_CODES[2]) == 'PERSISTENT_SEQUENTIAL'

    assert isinstance(CREATE_CODES[3], EphemeralSequential)
    assert CREATE_CODES[3].flags == 3
    assert CREATE_CODES[3].ephemeral == True
    assert CREATE_CODES[3].sequential == True
    assert str(CREATE_CODES[3]) == 'EPHEMERAL_SEQUENTIAL'


def test_EXCEPTIONS():
    assert isinstance(EXCEPTIONS[-1](), SystemZookeeperError)
    assert isinstance(EXCEPTIONS[-2](), RuntimeInconsistency)
    assert isinstance(EXCEPTIONS[-3](), DataInconsistency)
    assert isinstance(EXCEPTIONS[-4](), ConnectionLoss)
    assert isinstance(EXCEPTIONS[-5](), MarshallingError)
    assert isinstance(EXCEPTIONS[-6](), UnimplementedError)
    assert isinstance(EXCEPTIONS[-7](), OperationTimeoutError)
    assert isinstance(EXCEPTIONS[-8](), BadArgumentsError)
    assert isinstance(EXCEPTIONS[-100](), APIError)
    assert isinstance(EXCEPTIONS[-101](), NoNodeError)
    assert isinstance(EXCEPTIONS[-102](), NoAuthError)
    assert isinstance(EXCEPTIONS[-103](), BadVersionError)
    assert isinstance(EXCEPTIONS[-108](), NoChildrenForEphemeralsError)
    assert isinstance(EXCEPTIONS[-110](), NodeExistsError)
    assert isinstance(EXCEPTIONS[-111](), NotEmptyError)
    assert isinstance(EXCEPTIONS[-112](), SessionExpiredError)
    assert isinstance(EXCEPTIONS[-113](), InvalidCallbackError)
    assert isinstance(EXCEPTIONS[-114](), InvalidACLError)
    assert isinstance(EXCEPTIONS[-115](), AuthFailedError)

    try:
        EXCEPTIONS[666]()
        assert False, 'Non existent error code should have thrown an exception'
    except Exception:
        pass

CHROOT = '/acabrera'
HOSTS = 'localhost'

def test_hosts():
    hosts, root = collect_hosts('a:1/abc')
    assert root == '/abc'
    assert hosts.next() == ('a', 1)

    hosts, root = collect_hosts('a:12913')
    assert root == None
    assert hosts.next() == ('a', 12913)

    hosts, root = collect_hosts('a')
    assert root == None
    assert hosts.next() == ('a', 2181)

    hosts, root = collect_hosts('a/')
    assert root == None
    assert hosts.next() == ('a', 2181)

    hosts, root = collect_hosts('a/abc')
    assert root == '/abc'
    assert hosts.next() == ('a', 2181)

    hosts, root = collect_hosts('a:1,b:2,c/abc')
    assert root == '/abc'
    s = set()
    for host_port in hosts:
        s.add(host_port)
        if len(s) == 3: break
    assert ('a', 1) in s
    assert ('b', 2) in s
    assert ('c', 2181) in s

    count = 0
    previous_host_port = None
    for host_port in hosts:
        assert previous_host_port != host_port
        previous_host_port = host_port
        count = count + 1
        if count > 16: break
    assert count == 17


def test_prefix_root():
    def check_equal(a, b):
        assert a == b, '{} != {}'.format(a, b)

    for root, path, full_path in [
        ('', 'foo', '/foo'),
        ('', '/foo', '/foo'),
        ('/', 'foo', '/foo'),
        ('/moo/', '/foo/', '/moo/foo'),
        ('/moo', 'foo/', '/moo/foo'),
    ]:
        prefixed_root = _prefix_root(root, path)
        yield check_equal, prefixed_root, full_path


class Mine(Watcher):
    def __init__(self):
        pass

    def sessionConnected(self, session_id, session_password, read_only):
        print 'CONNECTED %r %r %r' % (session_id, _hex(session_password), read_only)

    def sessionExpired(self, session_id):
        print 'EXPIRED'

    def connectionDropped(self):
        print 'DROPPED'

    def connectionClosed(self):
        print 'CLOSED'


@attr('server')
@attr('slow')
def test_ping():
    hosts = HOSTS + CHROOT
    z = zookeeper.allocate(hosts, session_timeout=1.0, watcher=Mine())

    children, stat = z.get_children('/')

    time.sleep(10)

    children, stat = z.get_children('/')

    z.close()


@attr('server')
def test_zookeeper():
    hosts = HOSTS + CHROOT

    z = zookeeper.allocate(hosts)

    children, stat = z.get_children('/')
    children, stat = z.get_children('/')

    stat = z.exists('/pookie')
    if stat: z.delete('/pookie', stat.version)

    z.create('/pookie', CREATOR_ALL_ACL, Persistent())

    stat = z.exists('/pookie')
    stat = z.set_data('/pookie', bytearray([0] * 16), stat.version)

    data, stat = z.get_data('/pookie')
    assert data == bytearray([0] * 16)

    z.set_acls('/pookie', CREATOR_ALL_ACL + READ_ACL_UNSAFE, stat.aversion)
    acls, stat = z.get_acls('/pookie')
    assert len(acls) == 2
    for acl in acls:
        assert acl in set(CREATOR_ALL_ACL + READ_ACL_UNSAFE)

    z.sync('/pookie')
    z.delete('/pookie', stat.version)

    assert not z.exists('/pookie')

    z.close()


@attr('server')
def test_transaction():
    hosts = HOSTS + CHROOT

    z = zookeeper.allocate(hosts, watcher=Mine())

    # this should fail because /bar does not exist
    stat = z.exists('/foo')
    if stat: z.delete('/foo', stat.version)
    z.create('/foo', CREATOR_ALL_ACL, Persistent())
    stat = z.exists('/foo')
    transaction = z.allocate_transaction()
    transaction.create('/acabrera', CREATOR_ALL_ACL, Persistent())
    transaction.check('/foo', stat.version)
    transaction.check('/bar', stat.version)
    transaction.delete('/foo', stat.version)
    results = transaction.commit()

    assert not z.exists('/acabrera')
    assert z.exists('/foo')

    # this should succeed
    transaction = z.allocate_transaction()
    transaction.create('/acabrera', CREATOR_ALL_ACL, Persistent())
    transaction.check('/foo', stat.version)
    transaction.delete('/foo', stat.version)
    results = transaction.commit()

    stat = z.exists('/acabrera')
    assert stat
    z.delete('/acabrera', stat.version)
    assert not z.exists('/foo')

    z.close()


@attr('server')
def test_auth():
    hosts = HOSTS + CHROOT
    try:
        z = zookeeper.allocate(hosts, session_timeout=1.0, watcher=Mine(), auth_data=set([('a', 'b')]))
        assert False, 'Allocation should have thrown an AuthFailedError'
    except AuthFailedError:
        pass


def setup_module():
    logger = logging.getLogger('toolazydogs.zookeeper')

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(logging.Formatter('%(name)-12s[%(thread)d]: %(levelname)-8s %(message)s'))

    logger.addHandler(console)
