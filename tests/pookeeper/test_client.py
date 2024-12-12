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
import threading
import time

from mockito import mockito, inorder, matchers
from mockito.mocking import mock
from mockito.mockito import verifyNoMoreInteractions

from pookeeper import DropableClient34
from pookeeper.harness import PookeeperTestCase, zookeeper_version
import pookeeper
from pookeeper import CREATOR_ALL_ACL, Ephemeral, SessionExpiredError, ConnectionLoss, Watcher, AuthFailedError, Persistent, PersistentSequential, EphemeralSequential, READ_ACL_UNSAFE
from pookeeper import ConnectionDroppedForTest


LOGGER = logging.getLogger('toolazydogs.pookeeper.test')

class WatcherTests(PookeeperTestCase):
    def test_close(self):
        watcher = WatcherCounter()
        client = pookeeper.allocate(self.hosts, session_timeout=3.0, watcher=watcher)
        client.sync('/')
        client.close()

        assert watcher._session_connected == 1
        assert not watcher._session_expired
        assert not watcher._auth_failed
        assert not watcher._connection_dropped
        assert watcher._connection_closed == 1

    def test_exists_default_watcher(self):
        watcher = mock()
        z = pookeeper.allocate(self.hosts, watcher=watcher)

        assert not z.exists('/pookie', watch=True)
        z.create('/pookie', CREATOR_ALL_ACL, Ephemeral(), data=_random_data())

        stat = z.exists('/pookie', watch=True)
        stat = z.set_data('/pookie', _random_data(), stat.version)
        # This data change will be ignored since the watch has been reset
        z.set_data('/pookie', _random_data(), stat.version)
        stat = z.exists('/pookie', watch=True)
        z.delete('/pookie', stat.version)

        z.close()

        inorder.verify(watcher).session_connected(matchers.any(long), matchers.any(str), False)
        inorder.verify(watcher).node_created('/pookie')
        inorder.verify(watcher).data_changed('/pookie')
        inorder.verify(watcher).node_deleted('/pookie')
        inorder.verify(watcher).connection_closed()
        verifyNoMoreInteractions(watcher)

    def test_set_data_default_watcher(self):
        watcher = mock()
        z = pookeeper.allocate(self.hosts, watcher=watcher)

        z.create('/pookie', CREATOR_ALL_ACL, Ephemeral(), data=_random_data())

        stat = z.exists('/pookie')
        stat = z.set_data('/pookie', _random_data(), stat.version)
        z.get_data('/pookie', watch=True)
        stat = z.set_data('/pookie', _random_data(), stat.version)
        z.get_data('/pookie', watch=True)
        z.delete('/pookie', stat.version)

        z.close()

        inorder.verify(watcher).session_connected(matchers.any(long), matchers.any(str), False)
        inorder.verify(watcher).data_changed('/pookie')
        inorder.verify(watcher).node_deleted('/pookie')
        inorder.verify(watcher).connection_closed()
        verifyNoMoreInteractions(watcher)

    def test_get_children_default_watcher(self):
        watcher = mock()
        z = pookeeper.allocate(self.hosts, watcher=watcher)

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

        mockito.verify(watcher).session_connected(matchers.any(long), matchers.any(str), False)
        mockito.verify(watcher, times=2).children_changed('/pookie')
        mockito.verify(watcher).node_deleted('/pookie/bear')
        mockito.verify(watcher).connection_closed()
        verifyNoMoreInteractions(watcher)

    def test_exists_watcher(self):
        watcher = mock()
        z = pookeeper.allocate(self.hosts)

        assert not z.exists('/pookie', watcher=watcher)
        z.create('/pookie', CREATOR_ALL_ACL, Ephemeral(), data=_random_data())

        stat = z.exists('/pookie', watcher=watcher)
        stat = z.set_data('/pookie', _random_data(), stat.version)
        # This data change will be ignored since the watch has been reset
        z.set_data('/pookie', _random_data(), stat.version)
        stat = z.exists('/pookie', watcher=watcher)
        z.delete('/pookie', stat.version)

        z.close()

        inorder.verify(watcher).node_created('/pookie')
        inorder.verify(watcher).data_changed('/pookie')
        inorder.verify(watcher).node_deleted('/pookie')
        verifyNoMoreInteractions(watcher)

    def test_set_data_watcher(self):
        watcher = mock()
        z = pookeeper.allocate(self.hosts)

        z.create('/pookie', CREATOR_ALL_ACL, Ephemeral(), data=_random_data())

        stat = z.exists('/pookie')
        stat = z.set_data('/pookie', _random_data(), stat.version)
        z.get_data('/pookie', watcher=watcher)
        stat = z.set_data('/pookie', _random_data(), stat.version)
        z.get_data('/pookie', watcher=watcher)
        z.delete('/pookie', stat.version)

        z.close()

        inorder.verify(watcher).data_changed('/pookie')
        inorder.verify(watcher).node_deleted('/pookie')
        verifyNoMoreInteractions(watcher)

    def test_get_children_watcher(self):
        watcher = mock()
        z = pookeeper.allocate(self.hosts)

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

        mockito.verify(watcher, times=2).children_changed('/pookie')
        mockito.verify(watcher).node_deleted('/pookie/bear')
        verifyNoMoreInteractions(watcher)


class SessionTests(PookeeperTestCase):
    def test_ping(self):
        """ Make sure client connection is kept alive by behind the scenes pinging
        """

        client = pookeeper.allocate(self.hosts, session_timeout=0.8)
        client.sync('/')

        time.sleep(5)

        client.get_children('/')
        client.close()


    def test_session_resumption(self):
        """ Test session reconnect

        disconnect the client by killing the socket, not sending the session
        disconnect to the server as usual. This allows the test to verify
        disconnect handling
        """
        client = DropableClient34(self.hosts)
        client.create('/e', CREATOR_ALL_ACL, Ephemeral())
        client.drop()

        try:
            client.exists('/e')
            assert False, 'Connection dropped for test'
        except ConnectionDroppedForTest:
            pass

        resumed_client = pookeeper.allocate(self.hosts, session_timeout=client.session_timeout, session_id=client.session_id, session_passwd=client.session_passwd)
        stat = resumed_client.exists('/e')
        assert stat is not None

        resumed_client.close()

        try:
            resumed_client.exists('/e')
            assert False, 'Should have raised SessionExpiredError'
        except SessionExpiredError:
            pass

        new_client = self._get_client(session_timeout=resumed_client.session_timeout, session_id=resumed_client.session_id, session_passwd=resumed_client.session_passwd)
        stat = new_client.exists('/e')
        assert stat is None
        new_client.close()

    def test_session_move(self):
        """ Test session move

        Make sure that we cannot have two connections with the same
        session id.
        """

        old_client = None
        for server in self.cluster:
            session_timeout = old_client.session_timeout if old_client else 30.0
            session_id = old_client.session_id if old_client else None
            session_passwd = old_client.session_passwd if old_client else None

            new_client = DropableClient34(server.address, session_timeout=session_timeout, session_id=session_id, session_passwd=session_passwd, allow_reconnect=False)
            new_client.sync('/')
            new_client.set_data('/', _random_data())

            if old_client:
                try:
                    old_client.set_data('/', _random_data())
                    assert False, 'Session should have moved'
                except ConnectionLoss:
                    old_client.drop()

            old_client = new_client

        old_client.drop()

    def test_negotiated_session_timeout(self):
        """ Verify access to the negotiated session timeout
        """
        TICK_TIME = 2.0

        # validate typical case - requested == negotiated
        client = pookeeper.allocate(self.hosts, session_timeout=TICK_TIME * 4)
        client.sync('/')
        assert TICK_TIME * 4 == client.negotiated_session_timeout
        client.close()

        # validate lower limit
        client = pookeeper.allocate(self.hosts, session_timeout=TICK_TIME)
        client.sync('/')
        assert TICK_TIME * 2 == client.negotiated_session_timeout
        client.close()

        # validate upper limit
        client = pookeeper.allocate(self.hosts, session_timeout=TICK_TIME * 30)
        client.sync('/')
        assert TICK_TIME * 20 == client.negotiated_session_timeout
        client.close()

    def test_state_no_dupuplicate_reporting(self):
        """ Verify state change notifications are not duplicated

        This test makes sure that duplicate state changes are not communicated
        to the client watcher. For example we should not notify state as
        "disconnected" if the watch has already been disconnected. In general
        we don't consider a dup state notification if the event type is
        not "None" (ie non-None communicates an event).
        """

        watcher = WatcherCounter()
        client = pookeeper.allocate(self.hosts, session_timeout=3.0, watcher=watcher)

        client.sync('/')

        self.cluster.stop()

        time.sleep(5)

        assert watcher._session_connected == 1
        assert not watcher._session_expired
        assert not watcher._auth_failed
        assert watcher._connection_dropped == 1
        assert not watcher._connection_closed


class AuthTests(PookeeperTestCase):
    def test_bugus_auth(self):
        z = pookeeper.allocate(self.hosts, auth_data=set([('bogus', 'authdata')]))
        try:
            z.exists('/zookeeper')
            assert False, 'Allocation should have thrown an AuthFailedError'
        except AuthFailedError:
            pass

        try:
            z.exists('/zookeeper')
            assert False, 'Allocation should have thrown an AuthFailedError'
        except AuthFailedError:
            pass

        z.close()


class CreateCodeTests(PookeeperTestCase):
    def test_persistent(self):
        z = pookeeper.allocate(self.hosts)

        random_data = _random_data()
        z.create('/pookie', CREATOR_ALL_ACL, Persistent(), data=random_data)

        z.close()

        z = pookeeper.allocate(self.hosts)

        data, stat = z.get_data('/pookie')
        assert data == random_data

        z.delete('/pookie', stat.version)

        assert not z.exists('/pookie')

        z.close()

    def test_ephemeral(self):
        z = pookeeper.allocate(self.hosts)

        random_data = _random_data()
        z.create('/pookie', CREATOR_ALL_ACL, Ephemeral(), data=random_data)

        z.close()

        z = pookeeper.allocate(self.hosts)

        assert not z.exists('/pookie')

        z.close()

    def test_persistent_sequential(self):
        z = pookeeper.allocate(self.hosts)

        z.create('/root', CREATOR_ALL_ACL, Persistent())

        random_data = _random_data()
        result = z.create('/root/pookie', CREATOR_ALL_ACL, PersistentSequential(), data=random_data)
        children, _ = z.get_children('/root')
        assert len(children) == 1
        assert int(children[0][len('/root/pookie'):]) == 0

        z.close()

        z = pookeeper.allocate(self.hosts)

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

        z = pookeeper.allocate(self.hosts)
        children, _ = z.get_children('/root')
        assert len(children) == 2
        children = sorted(children)
        assert int(children[0][len('/root/pookie'):]) == 0
        assert int(children[1][len('/root/pookie'):]) == 1

        pookeeper.delete(z, '/root')

        assert not z.exists('/root')

        z.close()

    def test_ephemeral_sequential(self):
        z = pookeeper.allocate(self.hosts)

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

        z = pookeeper.allocate(self.hosts)

        children, _ = z.get_children('/root')
        assert len(children) == 0

        pookeeper.delete(z, '/root')

        assert not z.exists('/root')

        z.close()

    def test_data(self):
        z = pookeeper.allocate(self.hosts)

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

    def test_acls(self):
        z = pookeeper.allocate(self.hosts)

        z.create('/pookie', CREATOR_ALL_ACL + READ_ACL_UNSAFE, Persistent())
        acls, stat = z.get_acls('/pookie')
        assert len(acls) == 2
        for acl in acls:
            assert acl in set(CREATOR_ALL_ACL + READ_ACL_UNSAFE)

        z.delete('/pookie', stat.version)

        assert not z.exists('/pookie')

        z.close()

    @zookeeper_version('3.4')
    def test_transaction(self):
        z = pookeeper.allocate(self.hosts)

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


class WatcherCounter(Watcher):
    def __init__(self, session_connected=0, session_expired=0, auth_failed=0, connection_dropped=0, connection_closed=0):
        self._session_connected = session_connected
        self._session_expired = session_expired
        self._auth_failed = auth_failed
        self._connection_dropped = connection_dropped
        self._connection_closed = connection_closed
        self._state_lock = threading.RLock()

    def __repr__(self):
        return 'WatcherCounter(%s, %s, %s, %s, %s)' % (self._session_connected, self._session_expired, self._auth_failed, self._connection_dropped, self._connection_closed)

    def session_connected(self, session_id, session_password, read_only):
        with self._state_lock:
            self._session_connected += 1

    def session_expired(self, session_id):
        with self._state_lock:
            self._session_expired += 1

    def auth_failed(self):
        with self._state_lock:
            self._auth_failed += 1

    def connection_dropped(self):
        with self._state_lock:
            self._connection_dropped += 1

    def connection_closed(self):
        with self._state_lock:
            self._connection_closed += 1


def _random_data():
    size = random.randint(1, 16)
    data = bytearray([0] * size)
    for i in range(size):
        data[i] = random.randint(0, 255)
    return data

