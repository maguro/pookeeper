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

from pookeeper import CONNECTED, CONNECTED_RO, CONNECTING, CONNECTION_DROPPED_FOR_TEST
from pookeeper.zookeeper import Client34


class DropableClient34(Client34):
    def __init__(
        self,
        hosts,
        session_id=None,
        session_passwd=None,
        session_timeout=30.0,
        auth_data=None,
        read_only=False,
        watcher=None,
        allow_reconnect=True,
    ):
        super(DropableClient34, self).__init__(
            hosts, session_id, session_passwd, session_timeout, auth_data, read_only, watcher, allow_reconnect
        )

    def drop(self):
        assert self.state in set([CONNECTING, CONNECTED, CONNECTED_RO, CONNECTION_DROPPED_FOR_TEST])
        with self._state_lock:
            self._closed(CONNECTION_DROPPED_FOR_TEST)
            self._writer_thread.soc.close()
