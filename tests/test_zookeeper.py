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
import pytest

from pookeeper import (
    APIError,
    AuthFailed,
    AuthFailedError,
    BadArgumentsError,
    BadVersionError,
    CREATE_CODES,
    Connecting,
    ConnectionLoss,
    DataInconsistency,
    EXCEPTIONS,
    Ephemeral,
    EphemeralSequential,
    InvalidACLError,
    InvalidCallbackError,
    MarshallingError,
    NoAuthError,
    NoChildrenForEphemeralsError,
    NoNodeError,
    NodeExistsError,
    NotEmptyError,
    OperationTimeoutError,
    Persistent,
    PersistentSequential,
    RuntimeInconsistency,
    SessionExpiredError,
    SystemZookeeperError,
    UnimplementedError,
)
from pookeeper.hosts import collect_hosts
from pookeeper.zookeeper import _prefix_root
from tests import container


def test_zookeeper():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()
        # Use the connection_string to interact with Zookeeper
        print("Zookeeper connection string:", connection_string)
        print("Zookeeper connection IP:", zk.get_zookeeper_ip())


def test_state():
    assert Connecting() == Connecting()
    assert Connecting() != AuthFailed()


def test_create_codes():
    assert isinstance(CREATE_CODES[0], Persistent)
    assert CREATE_CODES[0].flags == 0
    assert CREATE_CODES[0].ephemeral == False
    assert CREATE_CODES[0].sequential == False
    assert str(CREATE_CODES[0]) == "PERSISTENT"

    assert isinstance(CREATE_CODES[1], Ephemeral)
    assert CREATE_CODES[1].flags == 1
    assert CREATE_CODES[1].ephemeral == True
    assert CREATE_CODES[1].sequential == False
    assert str(CREATE_CODES[1]) == "EPHEMERAL"

    assert isinstance(CREATE_CODES[2], PersistentSequential)
    assert CREATE_CODES[2].flags == 2
    assert CREATE_CODES[2].ephemeral == False
    assert CREATE_CODES[2].sequential == True
    assert str(CREATE_CODES[2]) == "PERSISTENT_SEQUENTIAL"

    assert isinstance(CREATE_CODES[3], EphemeralSequential)
    assert CREATE_CODES[3].flags == 3
    assert CREATE_CODES[3].ephemeral == True
    assert CREATE_CODES[3].sequential == True
    assert str(CREATE_CODES[3]) == "EPHEMERAL_SEQUENTIAL"


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
        assert False, "Non existent error code should have thrown an exception"
    except Exception:
        pass


def test_hosts():
    hosts, root = collect_hosts("a:1/abc")
    assert root == "/abc"
    assert hosts.__next__() == ("a", 1)

    hosts, root = collect_hosts("a:12913")
    assert root is None
    assert hosts.__next__() == ("a", 12913)

    hosts, root = collect_hosts("a")
    assert root is None
    assert hosts.__next__() == ("a", 2181)

    hosts, root = collect_hosts("a/")
    assert root is None
    assert hosts.__next__() == ("a", 2181)

    hosts, root = collect_hosts("a/abc")
    assert root == "/abc"
    assert hosts.__next__() == ("a", 2181)

    hosts, root = collect_hosts("a:1,b:2,c/abc")
    assert root == "/abc"
    s = set()
    for host_port in hosts:
        s.add(host_port)
        if len(s) == 3:
            break
    assert ("a", 1) in s
    assert ("b", 2) in s
    assert ("c", 2181) in s

    count = 0
    previous_host_port = None
    for host_port in hosts:
        assert previous_host_port != host_port
        previous_host_port = host_port
        count += 1
        if count > 16:
            break
    assert count == 17


@pytest.mark.parametrize("root, path, full_path",
                         [
                             ("", "foo", "/foo"),
                             ("", "/foo", "/foo"),
                             ("/", "foo", "/foo"),
                             ("/moo/", "/foo/", "/moo/foo"),
                             ("/moo", "foo/", "/moo/foo"),
                         ])
def test_prefix_root(root, path, full_path):
    prefixed_root = _prefix_root(root, path)
    assert prefixed_root == full_path, f"{prefixed_root} != {full_path}"
