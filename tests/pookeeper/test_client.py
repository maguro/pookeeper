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
import uuid

from pookeeper.harness import PookeeperTestCase
from toolazydogs.zookeeper.zookeeper import create


class  DataWatcherTests(PookeeperTestCase):
#    def setUp(self):
#        super(DataWatcherTests, self).setUp()
#        self.path = "/" + uuid.uuid4().hex
#        create(self.client, self.path)

    def test_ping(self):
        """ Make sure client connection is kept alive by behind the scenes pinging
        """

        time.sleep(5)

        self.client.get_children('/')




def _random_data():
    size = random.randint(1, 1024)
    data = bytearray([0] * size)
    for i in range(size):
        data[i] = random.randint(0, 255)
    return data

