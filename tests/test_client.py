# Copyright the original author or authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import base64
import hashlib
import logging
import random
import threading
import time

from mockito import inorder, matchers, mock, mockito, verifyNoMoreInteractions

import pookeeper
from pookeeper import (
    Watcher,
)
from pookeeper.impl import ConnectionDroppedForTest
from pookeeper.packets.data.ACL import ACL
from pookeeper.packets.data.Id import Id
from pookeeper.packets.data.Stat import Stat
from tests import (
    DropableClient34,
    container,
)

LOGGER = logging.getLogger("tests")


def test_ping():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()
        with pookeeper.allocate(connection_string, session_timeout=0.8) as client:
            client.sync("/")

            time.sleep(5)

            client.get_children("/")


def test_close():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()
        watcher = WatcherCounter()
        client = pookeeper.allocate(
            connection_string, session_timeout=3.0, watcher=watcher
        )
        client.sync("/")
        client.close()

        assert watcher._session_connected == 1
        assert not watcher._session_expired
        assert not watcher._auth_failed
        assert not watcher._connection_dropped
        assert watcher._connection_closed == 1


def test_exists_default_watcher():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()
        watcher = mock()
        with pookeeper.allocate(connection_string, watcher=watcher) as client:
            assert not client.exists("/pookie", watch=True)
            client.create(
                "/pookie",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.Ephemeral(),
                data=_random_data(),
            )

            stat = client.exists("/pookie", watch=True)
            stat = client.set_data("/pookie", _random_data(), stat.version)
            # This data change will be ignored since the watch has been reset
            client.set_data("/pookie", _random_data(), stat.version)
            stat = client.exists("/pookie", watch=True)
            client.delete("/pookie", stat.version)

        inorder.verify(watcher).session_connected(
            matchers.any(int), matchers.any(bytearray), False
        )
        inorder.verify(watcher).node_created("/pookie")
        inorder.verify(watcher).data_changed("/pookie")
        inorder.verify(watcher).node_deleted("/pookie")
        inorder.verify(watcher).connection_closed()
        verifyNoMoreInteractions(watcher)


def test_exists_without_watch_registers_no_watcher():
    """An unwatched exists() on a missing node must not register a watcher."""
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()
        default_watcher = mock()
        explicit_watcher = mock()
        with pookeeper.allocate(connection_string, watcher=default_watcher) as client:
            assert not client.exists("/pookie")
            assert not client.exists("/pookie", watcher=explicit_watcher)
            client.create(
                "/pookie",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.Ephemeral(),
                data=_random_data(),
            )
            stat = client.exists("/pookie")
            client.delete("/pookie", stat.version)

        mockito.verify(explicit_watcher).node_created("/pookie")
        verifyNoMoreInteractions(explicit_watcher)
        inorder.verify(default_watcher).session_connected(
            matchers.any(int), matchers.any(bytearray), False
        )
        inorder.verify(default_watcher).connection_closed()
        verifyNoMoreInteractions(default_watcher)


def test_set_data_default_watcher():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()
        watcher = mock()
        with pookeeper.allocate(connection_string, watcher=watcher) as client:
            client.create(
                "/pookie",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.Ephemeral(),
                data=_random_data(),
            )

            stat = client.exists("/pookie")
            stat = client.set_data("/pookie", _random_data(), stat.version)
            client.get_data("/pookie", watch=True)
            stat = client.set_data("/pookie", _random_data(), stat.version)
            client.get_data("/pookie", watch=True)
            client.delete("/pookie", stat.version)

    inorder.verify(watcher).session_connected(
        matchers.any(int), matchers.any(bytearray), False
    )
    inorder.verify(watcher).data_changed("/pookie")
    inorder.verify(watcher).node_deleted("/pookie")
    inorder.verify(watcher).connection_closed()
    verifyNoMoreInteractions(watcher)


def test_get_children_default_watcher():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()
        watcher = mock()
        with pookeeper.allocate(connection_string, watcher=watcher) as client:
            client.create(
                "/pookie",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.Persistent(),
                data=_random_data(),
            )
            client.get_children("/pookie", watch=True)

            client.create(
                "/pookie/bear",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.Persistent(),
                data=_random_data(),
            )
            client.get_children("/pookie", watch=True)

            client.set_data("/pookie", _random_data())
            client.set_data("/pookie/bear", _random_data())

            # One is for when we do and the other is for when we don't chroot
            client.get_children("/pookie", watch=True)
            client.get_children("/pookie/bear", watch=True)

            client.delete("/pookie/bear")
            client.delete("/pookie")

    mockito.verify(watcher).session_connected(
        matchers.any(int), matchers.any(bytearray), False
    )
    mockito.verify(watcher, times=2).children_changed("/pookie")
    mockito.verify(watcher).node_deleted("/pookie/bear")
    mockito.verify(watcher).connection_closed()
    verifyNoMoreInteractions(watcher)


def test_exists_watcher():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()
        watcher = mock()
        with pookeeper.allocate(connection_string, watcher=watcher) as client:
            assert not client.exists("/pookie", watcher=watcher)
            client.create(
                "/pookie",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.Ephemeral(),
                data=_random_data(),
            )

            stat = client.exists("/pookie", watcher=watcher)
            stat = client.set_data("/pookie", _random_data(), stat.version)
            # This data change will be ignored since the watch has been reset
            client.set_data("/pookie", _random_data(), stat.version)
            stat = client.exists("/pookie", watcher=watcher)
            client.delete("/pookie", stat.version)

    inorder.verify(watcher).session_connected(
        matchers.any(int), matchers.any(bytearray), False
    )
    inorder.verify(watcher).node_created("/pookie")
    inorder.verify(watcher).data_changed("/pookie")
    inorder.verify(watcher).node_deleted("/pookie")
    inorder.verify(watcher).connection_closed()
    verifyNoMoreInteractions(watcher)


def test_set_data_watcher():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()
        watcher = mock()
        with pookeeper.allocate(connection_string, watcher=watcher) as client:
            client.create(
                "/pookie",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.Ephemeral(),
                data=_random_data(),
            )

            stat = client.exists("/pookie")
            stat = client.set_data("/pookie", _random_data(), stat.version)
            client.get_data("/pookie", watcher=watcher)
            stat = client.set_data("/pookie", _random_data(), stat.version)
            client.get_data("/pookie", watcher=watcher)
            client.delete("/pookie", stat.version)

    inorder.verify(watcher).session_connected(
        matchers.any(int), matchers.any(bytearray), False
    )
    inorder.verify(watcher).data_changed("/pookie")
    inorder.verify(watcher).node_deleted("/pookie")
    inorder.verify(watcher).connection_closed()
    verifyNoMoreInteractions(watcher)


def test_get_children_watcher():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()
        watcher = mock()
        with pookeeper.allocate(connection_string, watcher=watcher) as client:
            client.create(
                "/pookie",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.Persistent(),
                data=_random_data(),
            )
            client.get_children("/pookie", watcher=watcher)

            client.create(
                "/pookie/bear",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.Persistent(),
                data=_random_data(),
            )
            client.get_children("/pookie", watcher=watcher)

            client.set_data("/pookie", _random_data())
            client.set_data("/pookie/bear", _random_data())

            # One is for when we do and the other is for when we don't chroot
            client.get_children("/pookie", watcher=watcher)
            client.get_children("/pookie/bear", watcher=watcher)

            client.delete("/pookie/bear")
            client.delete("/pookie")

    mockito.verify(watcher).session_connected(
        matchers.any(int), matchers.any(bytearray), False
    )
    mockito.verify(watcher, times=2).children_changed("/pookie")
    mockito.verify(watcher).node_deleted("/pookie/bear")
    mockito.verify(watcher).connection_closed()
    verifyNoMoreInteractions(watcher)


def test_session_ping():
    """Make sure client connection is kept alive by behind the scenes pinging"""
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()
        with pookeeper.allocate(connection_string, session_timeout=0.8) as client:
            client.sync("/")

            time.sleep(5)

            client.get_children("/")


def test_session_resumption():
    """Test session reconnect

    disconnect the client by killing the socket, not sending the session
    disconnect to the server as usual. This allows the test to verify
    disconnect handling
    """
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()

        client = DropableClient34(connection_string)
        client.create("/e", pookeeper.OPEN_ACL_UNSAFE, pookeeper.Ephemeral())
        client.drop()

        try:
            client.exists("/e")
            raise AssertionError("Connection dropped for test")
        except ConnectionDroppedForTest:
            pass

        resumed_client = pookeeper.allocate(
            connection_string,
            session_timeout=client.session_timeout,
            session_id=client.session.id,
            session_passwd=client.session.passwd,
        )
        stat = resumed_client.exists("/e")
        assert stat is not None

        resumed_client.close()

        try:
            resumed_client.exists("/e")
            raise AssertionError("Should have raised SessionExpiredError")
        except pookeeper.SessionExpiredError:
            pass

        with DropableClient34(
            connection_string,
            session_timeout=resumed_client.session_timeout,
            session_id=resumed_client.session.id,
            session_passwd=resumed_client.session.passwd,
        ) as new_client:
            stat = new_client.exists("/e")
            assert stat is None


def test_negotiated_session_timeout():
    """Verify access to the negotiated session timeout"""
    TICK_TIME = 2.0
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()

        # validate typical case - requested == negotiated
        with pookeeper.allocate(
            connection_string, session_timeout=TICK_TIME * 4
        ) as client:
            client.sync("/")
            assert client.negotiated_session_timeout == TICK_TIME * 4

        # validate lower limit
        with pookeeper.allocate(connection_string, session_timeout=TICK_TIME) as client:
            client.sync("/")
            assert client.negotiated_session_timeout == TICK_TIME * 2

        # validate upper limit
        with pookeeper.allocate(
            connection_string, session_timeout=TICK_TIME * 30
        ) as client:
            client.sync("/")
            assert client.negotiated_session_timeout == TICK_TIME * 20


def test_state_no_duplicate_reporting():
    """Verify state change notifications are not duplicated

    This test makes sure that duplicate state changes are not communicated
    to the client watcher. For example, we should not notify state as
    "disconnected" if the watch has already been disconnected. In general
    we don't consider a dup state notification if the event type is
    not "None" (ie non-None communicates an event).
    """
    zk = container.Zookeeper()
    zk.start()

    connection_string = zk.get_connection_string()
    watcher = WatcherCounter()

    client = pookeeper.allocate(connection_string, session_timeout=3.0, watcher=watcher)

    client.sync("/")

    zk.stop()

    time.sleep(10)

    assert watcher._session_connected == 1
    assert not watcher._session_expired
    assert not watcher._auth_failed
    assert watcher._connection_dropped == 1
    assert not watcher._connection_closed


def test_creator_all_acl_requires_authentication():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()

        with pookeeper.allocate(connection_string, session_timeout=0.8) as z:
            try:
                z.create("/pookie", pookeeper.CREATOR_ALL_ACL, pookeeper.Persistent())
                raise AssertionError("create should have raised InvalidACLError")
            except pookeeper.InvalidACLError:
                pass

            assert not z.exists("/pookie")


def test_creator_all_acl_restricts_node_to_creator():
    user, password = "pookie", "secret"
    digest = base64.b64encode(
        hashlib.sha1(f"{user}:{password}".encode()).digest()  # noqa: S324
    ).decode()

    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()

        with pookeeper.allocate(
            connection_string,
            session_timeout=0.8,
            auth_data=[("digest", bytearray(f"{user}:{password}".encode()))],
        ) as creator:
            creator.create(
                "/pookie",
                pookeeper.CREATOR_ALL_ACL,
                pookeeper.Persistent(),
                data=bytearray(b"secret data"),
            )

            acls, _ = creator.get_acls("/pookie")
            assert acls == [ACL(pookeeper.Perms.ALL, Id("digest", f"{user}:{digest}"))]

            with pookeeper.allocate(connection_string, session_timeout=0.8) as other:
                try:
                    other.get_data("/pookie")
                    raise AssertionError("get_data should have raised NoAuthError")
                except pookeeper.NoAuthError:
                    pass

            data, _ = creator.get_data("/pookie")
            assert data == bytearray(b"secret data")


def test_default_acl():
    """create() falls back to the client's default_acl, then OPEN_ACL_UNSAFE."""
    user, password = "pookie", "secret"
    digest = base64.b64encode(
        hashlib.sha1(f"{user}:{password}".encode()).digest()  # noqa: S324
    ).decode()
    creator_acl = [ACL(pookeeper.Perms.ALL, Id("digest", f"{user}:{digest}"))]

    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()

        with pookeeper.allocate(
            connection_string,
            session_timeout=0.8,
            auth_data=[("digest", bytearray(f"{user}:{password}".encode()))],
            default_acl=pookeeper.CREATOR_ALL_ACL,
        ) as z:
            assert z.create("/plain") == "/plain"
            assert z.get_acls("/plain")[0] == creator_acl

            pookeeper.create(z, "/parent/child")
            assert z.get_acls("/parent")[0] == creator_acl
            assert z.get_acls("/parent/child")[0] == creator_acl

            t = z.allocate_transaction()
            t.create("/in_transaction")
            t.commit()
            assert z.get_acls("/in_transaction")[0] == creator_acl

            assert z.create("/explicit", pookeeper.READ_ACL_UNSAFE) == "/explicit"
            assert z.get_acls("/explicit")[0] == pookeeper.READ_ACL_UNSAFE

            try:
                z.create("/empty", [])
                raise AssertionError("create should have raised InvalidACLError")
            except pookeeper.InvalidACLError:
                pass

        with pookeeper.allocate(connection_string, session_timeout=0.8) as z:
            assert z.create("/open") == "/open"
            assert z.get_acls("/open")[0] == pookeeper.OPEN_ACL_UNSAFE


def test_bogus_auth():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()

        with pookeeper.allocate(
            connection_string,
            session_timeout=0.8,
            auth_data=[("bogus", bytearray(b"authdata"))],
        ) as client:
            try:
                client.exists("/zookeeper")
                raise AssertionError("Allocation should have thrown an AuthFailedError")
            except pookeeper.AuthFailedError:
                pass

            try:
                client.exists("/zookeeper")
                raise AssertionError("Allocation should have thrown an AuthFailedError")
            except pookeeper.AuthFailedError:
                pass


def test_persistent():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()

        data_initial = _random_data()
        with pookeeper.allocate(connection_string, session_timeout=0.8) as client:
            client.create(
                "/pookie",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.Persistent(),
                data=data_initial,
            )

        with pookeeper.allocate(connection_string, session_timeout=0.8) as client:
            _data, stat = client.get_data("/pookie")
            assert isinstance(stat, Stat)
            assert stat.dataLength == len(data_initial)

            client.delete("/pookie", stat.version)

            assert not client.exists("/pookie")


def test_ephemeral():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()

        with pookeeper.allocate(connection_string, session_timeout=0.8) as z:
            random_data = _random_data()
            z.create(
                "/pookie",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.Ephemeral(),
                data=random_data,
            )

        with pookeeper.allocate(connection_string, session_timeout=0.8) as z:
            assert not z.exists("/pookie")


def test_ephemeral_sequential():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()

        with pookeeper.allocate(connection_string, session_timeout=0.8) as z:
            z.create("/root", pookeeper.OPEN_ACL_UNSAFE, pookeeper.Persistent())

            data_one = _random_data()
            path_one = z.create(
                "/root/pookie",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.EphemeralSequential(),
                data=data_one,
            )
            data_two = _random_data()
            path_two = z.create(
                "/root/pookie",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.EphemeralSequential(),
                data=data_two,
            )
            data_three = _random_data()
            path_three = z.create(
                "/root/pookie",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.EphemeralSequential(),
                data=data_three,
            )

            children, _ = z.get_children("/root")
            children = sorted(children)
            assert len(children) == 3
            assert children[0] == path_one[len("/root/") :]
            assert children[1] == path_two[len("/root/") :]
            assert children[2] == path_three[len("/root/") :]

            dat, _stat = z.get_data(path_one)
            assert dat == data_one
            dat, _stat = z.get_data(path_two)
            assert dat == data_two
            dat, _stat = z.get_data(path_three)
            assert dat == data_three

        with pookeeper.allocate(connection_string, session_timeout=0.8) as z:
            children, _ = z.get_children("/root")
            assert len(children) == 0

            pookeeper.delete(z, "/root")

            assert not z.exists("/root")


def test_persistent_sequential():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()

        with pookeeper.allocate(connection_string, session_timeout=0.8) as z:
            z.create("/root", pookeeper.OPEN_ACL_UNSAFE, pookeeper.Persistent())

            data_one = _random_data()
            path_one = z.create(
                "/root/pookie",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.PersistentSequential(),
                data=data_one,
            )
            children, _ = z.get_children("/root")
            assert len(children) == 1
            assert children[0] == path_one[len("/root/") :]

            dat, _stat = z.get_data(path_one)
            assert dat == data_one

        with pookeeper.allocate(connection_string, session_timeout=0.8) as z:
            children, _ = z.get_children("/root")
            assert len(children) == 1
            assert children[0] == path_one[len("/root/") :]
            dat, _stat = z.get_data(path_one)
            assert dat == data_one

            data_two = _random_data()
            path_two = z.create(
                "/root/pookie",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.PersistentSequential(),
                data=data_two,
            )

            children, _ = z.get_children("/root")
            assert len(children) == 2
            children = sorted(children)
            assert children[0] == path_one[len("/root/") :]
            assert children[1] == path_two[len("/root/") :]

            dat, _stat = z.get_data(path_one)
            assert dat == data_one
            dat, _stat = z.get_data(path_two)
            assert dat == data_two

        with pookeeper.allocate(connection_string, session_timeout=0.8) as z:
            children, _ = z.get_children("/root")
            assert len(children) == 2
            children = sorted(children)
            assert children[0] == path_one[len("/root/") :]
            assert children[1] == path_two[len("/root/") :]

            dat, _stat = z.get_data(path_one)
            assert dat == data_one
            dat, _stat = z.get_data(path_two)
            assert dat == data_two

            pookeeper.delete(z, "/root")

            assert not z.exists("/root")


def test_data():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()

        with pookeeper.allocate(connection_string, session_timeout=0.8) as z:
            random_data = _random_data()
            z.create(
                "/pookie",
                pookeeper.OPEN_ACL_UNSAFE,
                pookeeper.Persistent(),
                data=random_data,
            )

            data, stat = z.get_data("/pookie")
            assert data == random_data

            new_random_data = _random_data()
            stat = z.exists("/pookie")
            z.set_data("/pookie", new_random_data, stat.version)

            data, stat = z.get_data("/pookie")
            assert data == new_random_data

            z.delete("/pookie", stat.version)

            assert not z.exists("/pookie")


def test_acls():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()

        with pookeeper.allocate(connection_string, session_timeout=0.8) as z:
            z.create(
                "/pookie",
                pookeeper.OPEN_ACL_UNSAFE + pookeeper.READ_ACL_UNSAFE,
                pookeeper.Persistent(),
            )
            acls, stat = z.get_acls("/pookie")
            assert len(acls) == 2
            for acl in acls:
                assert acl in set(pookeeper.OPEN_ACL_UNSAFE + pookeeper.READ_ACL_UNSAFE)

            z.delete("/pookie", stat.version)

            assert not z.exists("/pookie")


def test_transaction():
    with container.Zookeeper(version="3.4.13") as zk:
        connection_string = zk.get_connection_string()

        with pookeeper.allocate(connection_string, session_timeout=0.8) as z:
            # this should fail because /bar does not exist
            z.create("/foo", pookeeper.OPEN_ACL_UNSAFE, pookeeper.Persistent())
            stat = z.exists("/foo")

            transaction = z.allocate_transaction()
            transaction.create(
                "/pookie", pookeeper.OPEN_ACL_UNSAFE, pookeeper.Persistent()
            )
            transaction.check("/foo", stat.version)
            transaction.check("/bar", stat.version)
            transaction.delete("/foo", stat.version)
            transaction.commit()

            assert not z.exists("/pookie")
            assert z.exists("/foo")

            # this should succeed
            transaction = z.allocate_transaction()
            transaction.create(
                "/pookie", pookeeper.OPEN_ACL_UNSAFE, pookeeper.Persistent()
            )
            transaction.check("/foo", stat.version)
            transaction.delete("/foo", stat.version)
            transaction.commit()

            try:
                transaction.create(
                    "/pookie", pookeeper.OPEN_ACL_UNSAFE, pookeeper.Persistent()
                )
                raise AssertionError(
                    "Transaction already committed - create should have failed"
                )
            except ValueError:
                pass
            try:
                transaction.check("/foo", stat.version)
                raise AssertionError(
                    "Transaction already committed - check should have failed"
                )
            except ValueError:
                pass
            try:
                transaction.delete("/foo", stat.version)
                raise AssertionError(
                    "Transaction already committed - delete should have failed"
                )
            except ValueError:
                pass
            try:
                transaction.commit()
                raise AssertionError(
                    "Transaction already committed - commit should have failed"
                )
            except ValueError:
                pass

            stat = z.exists("/pookie")
            z.delete("/pookie", stat.version)
            assert not z.exists("/foo")

            # test with
            z.create("/foo", pookeeper.OPEN_ACL_UNSAFE, pookeeper.Persistent())
            with z.allocate_transaction() as t:
                t.create("/pookie", pookeeper.OPEN_ACL_UNSAFE, pookeeper.Persistent())
                t.check("/foo", stat.version)
                t.delete("/foo", stat.version)

            stat = z.exists("/pookie")
            z.delete("/pookie", stat.version)
            assert not z.exists("/foo")


# class SessionTests(PookeeperTestCase):
#
#     def test_session_move(self):
#         """Test session move
#
#         Make sure that we cannot have two connections with the same
#         session id.
#         """
#
#         old_client = None
#         for server in self.cluster:
#             session_timeout = old_client.session_timeout if old_client else 30.0
#             session_id = old_client.session_id if old_client else None
#             session_passwd = old_client.session_passwd if old_client else None
#
#             new_client = DropableClient34(
#                 server.address,
#                 session_timeout=session_timeout,
#                 session_id=session_id,
#                 session_passwd=session_passwd,
#                 allow_reconnect=False,
#             )
#             new_client.sync("/")
#             new_client.set_data("/", _random_data())
#
#             if old_client:
#                 try:
#                     old_client.set_data("/", _random_data())
#                     assert False, "Session should have moved"
#                 except ConnectionLoss:
#                     old_client.drop()
#
#             old_client = new_client
#
#         old_client.drop()
#


class WatcherCounter(Watcher):
    def __init__(
        self,
        session_connected=0,
        session_expired=0,
        auth_failed=0,
        connection_dropped=0,
        connection_closed=0,
    ):
        self._session_connected = session_connected
        self._session_expired = session_expired
        self._auth_failed = auth_failed
        self._connection_dropped = connection_dropped
        self._connection_closed = connection_closed
        self._state_lock = threading.RLock()

    def __repr__(self):
        return (
            "WatcherCounter("
            f"{self._session_connected}, "
            f"{self._session_expired}, "
            f"{self._auth_failed}, "
            f"{self._connection_dropped}, "
            f"{self._connection_closed})"
        )

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
