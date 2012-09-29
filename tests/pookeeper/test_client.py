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
import random
import time

from pookeeper.harness import PookeeperTestCase
from toolazydogs.pookeeper import CREATOR_ALL_ACL, Ephemeral, SessionExpiredError, OPEN_ACL_UNSAFE, ConnectionLoss
from toolazydogs.pookeeper.impl import ConnectionDroppedForTest


class  SessionTests(PookeeperTestCase):
    def test_ping(self):
        """ Make sure client connection is kept alive by behind the scenes pinging
        """

        time.sleep(5)

        self.client.get_children('/')


    def test_session_resumption(self):
        """ Test session reconnect

        disconnect the client by killing the socket, not sending the session
        disconnect to the server as usual. This allows the test to verify
        disconnect handling
        """
        self.client.create('/e', CREATOR_ALL_ACL, Ephemeral())
        self.client.drop()

        try:
            self.client.exists('/e')
            assert False, 'Connection dropped for test'
        except ConnectionDroppedForTest:
            pass

        resumed_client = self._get_client(session_timeout=self.client.session_timeout, session_id=self.client.session_id, session_passwd=self.client.session_passwd)
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
        self.client.create('/sessionMoveTest', OPEN_ACL_UNSAFE, Ephemeral())

        new_client = self._get_client(session_timeout=self.client.session_timeout, session_id=self.client.session_id, session_passwd=self.client.session_passwd)
        new_client.sync('/')
        new_client.set_data('/', _random_data())

        try:
            self.client.set_data('/', _random_data())
            assert False, 'Session should have moved'
        except ConnectionLoss:
            self.client.close()

        new_client.close()


def _random_data():
    size = random.randint(1, 1024)
    data = bytearray([0] * size)
    for i in range(size):
        data[i] = random.randint(0, 255)
    return data

