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

from mockito import any, inorder, mock
import mockito
from mockito.mockito import verifyNoMoreInteractions
from nose.plugins.attrib import attr

from toolazydogs import zookeeper
from toolazydogs.zookeeper import  Persistent, AuthFailedError, CREATOR_ALL_ACL, READ_ACL_UNSAFE, Ephemeral, PersistentSequential, EphemeralSequential


HOSTS = 'localhost'

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
    z = zookeeper.allocate(HOSTS, auth_data=set([('a', 'b')]))
    try:
        z.exists('/zookeeper')
        assert False, 'Allocation should have thrown an AuthFailedError'
    except AuthFailedError:
        pass
    finally:
        z.close()


class Test(object):
    def __init__(self, chroot=None):
        self.chroot = chroot or ''
        z = zookeeper.allocate(HOSTS + self.chroot)
        _delete(z, '/pookeeper')
        _delete(z, '/pookie')
        _delete(z, '/root')
        _delete(z, '/foo')
        z.close()

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
        assert len(children) == 1
        assert int(children[0][len('/root/pookie'):]) == 0

        z.close()

        z = zookeeper.allocate(hosts)

        children, _ = z.get_children('/root')
        assert len(children) == 1
        assert int(children[0][len('/root/pookie'):]) == 0

        result = z.create('/root/pookie', CREATOR_ALL_ACL, PersistentSequential(), data=random_data)

        children, _ = z.get_children('/root')
        assert len(children) == 2
        children = sorted(children)
        assert int(children[0][len('/root/pookie'):]) == 0
        assert int(children[1][len('/root/pookie'):]) == 1

        z.close()

        z = zookeeper.allocate(hosts)
        children, _ = z.get_children('/root')
        assert len(children) == 2
        children = sorted(children)
        assert int(children[0][len('/root/pookie'):]) == 0
        assert int(children[1][len('/root/pookie'):]) == 1

        _delete(z, '/root')

        assert not z.exists('/root')

        z.close()

    @attr('server')
    def test_ephemeral_sequential(self):
        hosts = HOSTS + self.chroot

        z = zookeeper.allocate(hosts)

        z.create('/root', CREATOR_ALL_ACL, Persistent())

        random_data = _random_data()
        result = z.create('/root/pookie', CREATOR_ALL_ACL, EphemeralSequential(), data=random_data)
        result = z.create('/root/pookie', CREATOR_ALL_ACL, EphemeralSequential(), data=random_data)
        result = z.create('/root/pookie', CREATOR_ALL_ACL, EphemeralSequential(), data=random_data)

        children, _ = z.get_children('/root')
        children = sorted(children)
        assert len(children) == 3
        assert int(children[0][len('/root/pookie'):]) == 0
        assert int(children[1][len('/root/pookie'):]) == 1
        assert int(children[2][len('/root/pookie'):]) == 2

        z.close()

        z = zookeeper.allocate(hosts)

        children, _ = z.get_children('/root')
        assert len(children) == 0

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

        z = zookeeper.allocate(hosts)

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

        try:
            transaction.create('/pookie', CREATOR_ALL_ACL, Persistent())
            assert False, 'Transaction already committed - create should have failed'
        except ValueError:
            pass
        try:
            transaction.check('/foo', stat.version)
            assert False, 'Transaction already committed - check should have failed'
        except ValueError:
            pass
        try:
            transaction.delete('/foo', stat.version)
            assert False, 'Transaction already committed - delete should have failed'
        except ValueError:
            pass
        try:
            transaction.commit()
            assert False, 'Transaction already committed - commit should have failed'
        except ValueError:
            pass

        stat = z.exists('/pookie')
        z.delete('/pookie', stat.version)
        assert not z.exists('/foo')

        # test with
        z.create('/foo', CREATOR_ALL_ACL, Persistent())
        with z.allocate_transaction() as t:
            t.create('/pookie', CREATOR_ALL_ACL, Persistent())
            t.check('/foo', stat.version)
            t.delete('/foo', stat.version)

        stat = z.exists('/pookie')
        z.delete('/pookie', stat.version)
        assert not z.exists('/foo')

        z.close()

    @attr('server')
    def test_exists_default_watcher(self):
        hosts = HOSTS + self.chroot

        watcher = mock()
        z = zookeeper.allocate(hosts, watcher=watcher)

        assert not z.exists('/pookie', watch=True)
        z.create('/pookie', CREATOR_ALL_ACL, Ephemeral(), data=_random_data())

        stat = z.exists('/pookie', watch=True)
        stat = z.set_data('/pookie', _random_data(), stat.version)
        # This data change will be ignored since the watch has been reset
        z.set_data('/pookie', _random_data(), stat.version)
        stat = z.exists('/pookie', watch=True)
        z.delete('/pookie', stat.version)

        z.close()

        inorder.verify(watcher).session_connected(any(long), any(str), False)
        inorder.verify(watcher).node_created(self.chroot + '/pookie')
        inorder.verify(watcher).data_changed(self.chroot + '/pookie')
        inorder.verify(watcher).node_deleted(self.chroot + '/pookie')
        inorder.verify(watcher).connection_closed()
        verifyNoMoreInteractions(watcher)

    @attr('server')
    def test_set_data_default_watcher(self):
        hosts = HOSTS + self.chroot

        watcher = mock()
        z = zookeeper.allocate(hosts, watcher=watcher)

        z.create('/pookie', CREATOR_ALL_ACL, Ephemeral(), data=_random_data())

        stat = z.exists('/pookie')
        stat = z.set_data('/pookie', _random_data(), stat.version)
        z.get_data('/pookie', watch=True)
        stat = z.set_data('/pookie', _random_data(), stat.version)
        z.get_data('/pookie', watch=True)
        z.delete('/pookie', stat.version)

        z.close()

        inorder.verify(watcher).session_connected(any(long), any(str), False)
        inorder.verify(watcher).data_changed(self.chroot + '/pookie')
        inorder.verify(watcher).node_deleted(self.chroot + '/pookie')
        inorder.verify(watcher).connection_closed()
        verifyNoMoreInteractions(watcher)

    @attr('server')
    def test_get_children_default_watcher(self):
        hosts = HOSTS + self.chroot

        watcher = mock()
        z = zookeeper.allocate(hosts, watcher=watcher)

        z.create('/pookie', CREATOR_ALL_ACL, Persistent(), data=_random_data())
        z.get_children('/pookie', watch=True)

        z.create('/pookie/bear', CREATOR_ALL_ACL, Persistent(), data=_random_data())
        z.get_children('/pookie', watch=True)

        z.set_data('/pookie', _random_data())
        z.set_data('/pookie/bear', _random_data())

        # One is for when we do and the other is for when we don't chroot
        z.get_children('/pookie', watch=True)
        z.get_children('/pookie/bear', watch=True)

        z.delete('/pookie/bear')
        z.delete('/pookie')

        z.close()

        mockito.verify(watcher).session_connected(any(long), any(str), False)
        mockito.verify(watcher, times=2).children_changed(self.chroot + '/pookie')
        mockito.verify(watcher).node_deleted(self.chroot + '/pookie/bear')
        mockito.verify(watcher).connection_closed()
        verifyNoMoreInteractions(watcher)

    @attr('server')
    def test_exists_watcher(self):
        hosts = HOSTS + self.chroot

        watcher = mock()
        z = zookeeper.allocate(hosts)

        assert not z.exists('/pookie', watcher=watcher)
        z.create('/pookie', CREATOR_ALL_ACL, Ephemeral(), data=_random_data())

        stat = z.exists('/pookie', watcher=watcher)
        stat = z.set_data('/pookie', _random_data(), stat.version)
        # This data change will be ignored since the watch has been reset
        z.set_data('/pookie', _random_data(), stat.version)
        stat = z.exists('/pookie', watcher=watcher)
        z.delete('/pookie', stat.version)

        z.close()

        inorder.verify(watcher).node_created(self.chroot + '/pookie')
        inorder.verify(watcher).data_changed(self.chroot + '/pookie')
        inorder.verify(watcher).node_deleted(self.chroot + '/pookie')
        verifyNoMoreInteractions(watcher)

    @attr('server')
    def test_set_data_watcher(self):
        hosts = HOSTS + self.chroot

        watcher = mock()
        z = zookeeper.allocate(hosts)

        z.create('/pookie', CREATOR_ALL_ACL, Ephemeral(), data=_random_data())

        stat = z.exists('/pookie')
        stat = z.set_data('/pookie', _random_data(), stat.version)
        z.get_data('/pookie', watcher=watcher)
        stat = z.set_data('/pookie', _random_data(), stat.version)
        z.get_data('/pookie', watcher=watcher)
        z.delete('/pookie', stat.version)

        z.close()

        inorder.verify(watcher).data_changed(self.chroot + '/pookie')
        inorder.verify(watcher).node_deleted(self.chroot + '/pookie')
        verifyNoMoreInteractions(watcher)

    @attr('server')
    def test_get_children_watcher(self):
        hosts = HOSTS + self.chroot

        watcher = mock()
        z = zookeeper.allocate(hosts)

        z.create('/pookie', CREATOR_ALL_ACL, Persistent(), data=_random_data())
        z.get_children('/pookie', watcher=watcher)

        z.create('/pookie/bear', CREATOR_ALL_ACL, Persistent(), data=_random_data())
        z.get_children('/pookie', watcher=watcher)

        z.set_data('/pookie', _random_data())
        z.set_data('/pookie/bear', _random_data())

        # One is for when we do and the other is for when we don't chroot
        z.get_children('/pookie', watcher=watcher)
        z.get_children('/pookie/bear', watcher=watcher)

        z.delete('/pookie/bear')
        z.delete('/pookie')

        z.close()

        mockito.verify(watcher, times=2).children_changed(self.chroot + '/pookie')
        mockito.verify(watcher).node_deleted(self.chroot + '/pookie/bear')
        verifyNoMoreInteractions(watcher)


class TestChroot(Test):
    def __init__(self):
        Test.__init__(self, chroot='/pookeeper')

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
    #console.setLevel(logging.NOTSET)
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
