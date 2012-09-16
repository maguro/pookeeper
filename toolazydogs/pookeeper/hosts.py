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


class RandomHostIterator:
    """ An iterator that returns a randomly selected host.  A host is
        guaranteed to not be selected twice unless there is only one
        host in the collection.
    """

    def __init__(self, hosts):
        self.last = 0
        self.hosts = hosts

    def __iter__(self):
        return self

    def __len__(self):
        return len(self.hosts)

    def next(self):
        selected = self.last
        if (len(self.hosts) > 1):
            while selected == self.last:
                selected = random.randint(0, len(self.hosts) - 1)
            self.last = selected
        return self.hosts[selected]


def collect_hosts(hosts):
    """ Collect a set of hosts and an optional chroot from a string.
    """
    host_ports, chroot = hosts.partition("/")[::2]
    chroot = "/" + chroot if chroot else None

    result = []
    for host_port in host_ports.split(","):
        host, port = host_port.partition(":")[::2]
        port = int(port.strip()) if port else 2181
        result.append((host.strip(), port))
    return (RandomHostIterator(result), chroot)
