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
from toolazydogs.pookeeper import CREATOR_ALL_ACL, Ephemeral, SessionExpiredError
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
        self.client._drop()

        try:
            self.client.exists('/e')
            assert False, 'Connection dropped for test'
        except ConnectionDroppedForTest:
            pass

        self.client = self._get_client(session_timeout=self.client.session_timeout, session_id=self.client.session_id, session_passwd=self.client.session_passwd)
        stat = self.client.exists('/e')
        assert stat is not None

        self.client.close()

        try:
            self.client.exists('/e')
            assert False, 'Should have raised SessionExpiredError'
        except SessionExpiredError:
            pass

        self.client = self._get_client(session_timeout=self.client.session_timeout, session_id=self.client.session_id, session_passwd=self.client.session_passwd)
        stat = self.client.exists('/e')
        assert stat is None


def _random_data():
    size = random.randint(1, 1024)
    data = bytearray([0] * size)
    for i in range(size):
        data[i] = random.randint(0, 255)
    return data

