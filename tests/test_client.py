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
import random
import time

from nose.plugins.attrib import attr

from toolazydogs import zookeeper
from toolazydogs.zookeeper import  Persistent, AuthFailedError, Watcher, CREATOR_ALL_ACL, READ_ACL_UNSAFE, Ephemeral, PersistentSequential
from toolazydogs.zookeeper.impl import _hex


HOSTS = 'localhost'

class Mine(Watcher):
    def __init__(self):
        pass

    def sessionConnected(self, session_id, session_password, read_only):
        pass

    def sessionExpired(self, session_id):
        pass

    def connectionDropped(self):
        pass

    def connectionClosed(self):
        pass


@attr('server')
@attr('slow')
def test_ping():
    """ Make sure client connection is kept alive by behind the scenes pinging
    """
    z = zookeeper.allocate(HOSTS, session_timeout=1.0)

    time.sleep(10)

    z.get_children('/')

    z.close()


@attr('server')
def test_auth():
    try:
        zookeeper.allocate(HOSTS, auth_data=set([('a', 'b')]))
        assert False, 'Allocation should have thrown an AuthFailedError'
    except AuthFailedError:
        pass


class Test(object):
    def __init__(self):
        self.chroot = ''

    @attr('server')
    def test_persistent(self):
        hosts = HOSTS + self.chroot

        z = zookeeper.allocate(hosts)

        random_data = _random_data()
        z.create('/pookie', CREATOR_ALL_ACL, Persistent(), data=random_data)

        z.close()

        z = zookeeper.allocate(hosts)

        data, stat = z.get_data('/pookie')
        assert data == random_data

        z.delete('/pookie', stat.version)

        assert not z.exists('/pookie')

        z.close()

    @attr('server')
    def test_ephemeral(self):
        hosts = HOSTS + self.chroot

        z = zookeeper.allocate(hosts)

        random_data = _random_data()
        z.create('/pookie', CREATOR_ALL_ACL, Ephemeral(), data=random_data)

        z.close()

        z = zookeeper.allocate(hosts)

        assert not z.exists('/pookie')

        z.close()

    @attr('server')
    def test_persistent_sequential(self):
        hosts = HOSTS + self.chroot

        z = zookeeper.allocate(hosts)

        z.create('/root', CREATOR_ALL_ACL, Persistent())

        random_data = _random_data()
        result = z.create('/root/pookie', CREATOR_ALL_ACL, PersistentSequential(), data=random_data)
        children, _ = z.get_children('/root')
        count = 0
        for child in children:
            assert int(child[len('/root/pookie'):]) == count
            count += 1

        z.close()

        z = zookeeper.allocate(hosts)

        children, _ = z.get_children('/root')
        assert len(children) == 1
        count = 0
        for child in children:
            assert int(child[len('/root/pookie'):]) == count
            count += 1

        result = z.create('/root/pookie', CREATOR_ALL_ACL, PersistentSequential(), data=random_data)

        children, _ = z.get_children('/root')
        assert len(children) == 2
        count = 0
        for child in sorted(children):
            assert int(child[len('/root/pookie'):]) == count
            count += 1

        z.close()

        z = zookeeper.allocate(hosts)
        assert len(children) == 2
        children, _ = z.get_children('/root')
        count = 0
        for child in children:
            assert int(child[len('/root/pookie'):]) == count
            count += 1

        _delete(z, '/root')

        assert not z.exists('/root')

        z.close()

    @attr('server')
    def test_data(self):
        hosts = HOSTS + self.chroot

        z = zookeeper.allocate(hosts)

        random_data = _random_data()
        z.create('/pookie', CREATOR_ALL_ACL, Persistent(), data=random_data)

        data, stat = z.get_data('/pookie')
        assert data == random_data

        new_random_data = _random_data()
        stat = z.exists('/pookie')
        z.set_data('/pookie', new_random_data, stat.version)

        data, stat = z.get_data('/pookie')
        assert data == new_random_data

        z.delete('/pookie', stat.version)

        assert not z.exists('/pookie')

        z.close()

    @attr('server')
    def test_acls(self):
        hosts = HOSTS + self.chroot

        z = zookeeper.allocate(hosts)

        z.create('/pookie', CREATOR_ALL_ACL + READ_ACL_UNSAFE, Persistent())
        acls, stat = z.get_acls('/pookie')
        assert len(acls) == 2
        for acl in acls:
            assert acl in set(CREATOR_ALL_ACL + READ_ACL_UNSAFE)

        z.delete('/pookie', stat.version)

        assert not z.exists('/pookie')

        z.close()

    @attr('server')
    def test_transaction(self):
        hosts = HOSTS + self.chroot

        z = zookeeper.allocate(hosts, watcher=Mine())

        # this should fail because /bar does not exist
        z.create('/foo', CREATOR_ALL_ACL, Persistent())
        stat = z.exists('/foo')

        transaction = z.allocate_transaction()
        transaction.create('/pookie', CREATOR_ALL_ACL, Persistent())
        transaction.check('/foo', stat.version)
        transaction.check('/bar', stat.version)
        transaction.delete('/foo', stat.version)
        transaction.commit()

        assert not z.exists('/pookie')
        assert z.exists('/foo')

        # this should succeed
        transaction = z.allocate_transaction()
        transaction.create('/pookie', CREATOR_ALL_ACL, Persistent())
        transaction.check('/foo', stat.version)
        transaction.delete('/foo', stat.version)
        transaction.commit()

        stat = z.exists('/pookie')
        z.delete('/pookie', stat.version)
        assert not z.exists('/foo')

        z.close()


class TestChroot(Test):
    def __init__(self):
        Test.__init__(self)
        self.chroot = '/pookeeper'

    def setUp(self):
        z = zookeeper.allocate(HOSTS)
        z.create('/pookeeper', CREATOR_ALL_ACL, Persistent())
        z.close()

    def tearDown(self):
        z = zookeeper.allocate(HOSTS)
        _delete(z, '/pookeeper')
        z.close()


def setup_module():
    logger = logging.getLogger('toolazydogs.zookeeper')

    console = logging.StreamHandler()
    console.setLevel(logging.CRITICAL)
    console.setFormatter(logging.Formatter('%(name)-12s[%(thread)d]: %(levelname)-8s %(message)s'))

    logger.addHandler(console)


def _random_data():
    size = random.randint(1, 1024)
    data = bytearray([0] * size)
    for i in range(size):
        data[i] = random.randint(0, 255)
    return data


def _delete(z, path):
    if not z.exists(path): return

    children, stat = z.get_children(path)
    for child in children:
        _delete(z, path + '/' + child)
    z.delete(path, stat.version)