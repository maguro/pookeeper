import random

from mockito import inorder, matchers, mock, verifyNoMoreInteractions

import pookeeper
from pookeeper.packets.data.Stat import Stat

"""
Copyright 2024 the original author or authors

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
from tests import container


def test_zookeeper_container():
    with container.Zookeeper() as zk:
        connection_string = zk.get_connection_string()
        watcher = mock()

        with pookeeper.allocate(connection_string, session_timeout=0.8, watcher=watcher) as client:
            assert not client.exists("/pookie", watch=True)

            data_initial = _random_data()
            path = client.create("/pookie", pookeeper.CREATOR_ALL_ACL, pookeeper.Persistent(), data=data_initial)
            assert path == "/pookie"

            stat = client.exists("/pookie", watch=True)
            assert isinstance(stat, Stat)
            assert stat.dataLength == len(data_initial)

            acls, stat = client.get_acls("/pookie")
            assert isinstance(stat, Stat)
            assert acls == pookeeper.CREATOR_ALL_ACL
            assert stat.version == 0

            stat = client.set_acls("/pookie", pookeeper.CREATOR_ALL_ACL + pookeeper.READ_ACL_UNSAFE)
            assert isinstance(stat, Stat)

            acls, stat = client.get_acls("/pookie")
            assert isinstance(stat, Stat)
            assert acls == pookeeper.CREATOR_ALL_ACL + pookeeper.READ_ACL_UNSAFE
            assert stat.version == 0

            data_one = _random_data()
            stat = client.set_data("/pookie", data_one, stat.version)
            assert isinstance(stat, Stat)
            assert stat.dataLength == len(data_one)
            assert stat.version == 1

            data, stat = client.get_data("/pookie", watch=True)
            assert isinstance(stat, Stat)
            assert stat.dataLength == len(data_one)
            assert data == data_one

            data_two = _random_data()
            stat = client.set_data("/pookie", data_two, version=stat.version)
            assert isinstance(stat, Stat)
            assert stat.dataLength == len(data_two)
            assert stat.version == 2

            data, stat = client.get_data("/pookie", watch=True)
            assert isinstance(stat, Stat)
            assert stat.version == 2
            assert stat.dataLength == len(data_two)
            assert data == data_two

            data_bear = _random_data()
            path = client.create("/pookie/bear", pookeeper.CREATOR_ALL_ACL, pookeeper.Persistent(), data=data_bear)
            assert path == "/pookie/bear"

            data, stat = client.get_data("/pookie/bear", watch=True)
            assert isinstance(stat, Stat)
            assert stat.version == 0
            assert stat.dataLength == len(data_bear)
            assert data == data_bear

            data, stat = client.get_data("/pookie", watch=True)
            assert isinstance(stat, Stat)
            assert stat.version == 2
            assert stat.dataLength == len(data_two)
            assert data == data_two

            children, stat = client.get_children("/pookie", watch=True)
            assert isinstance(stat, Stat)
            assert stat.version == 2
            assert children == ["bear"]

            client.delete("/pookie/bear", 0)
            client.delete("/pookie", 2)

            assert not client.exists("/pookie", watch=True)

        inorder.verify(watcher).session_connected(matchers.any(int), matchers.any(bytearray), False)
        inorder.verify(watcher).node_created("/pookie")
        inorder.verify(watcher, times=2).data_changed("/pookie")
        inorder.verify(watcher, times=2).data_changed("/pookie")
        inorder.verify(watcher).node_deleted("/pookie/bear")
        inorder.verify(watcher).children_changed("/pookie")
        inorder.verify(watcher).node_deleted("/pookie")
        inorder.verify(watcher).connection_closed()
        verifyNoMoreInteractions(watcher)


def _random_data():
    size = random.randint(1, 16)
    data = bytearray([0] * size)
    for i in range(size):
        data[i] = random.randint(0, 255)
    return data
