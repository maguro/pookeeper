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

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs


class Zookeeper(DockerContainer):
    def __init__(self, image="zookeeper:latest"):
        super().__init__(image)
        self.with_bind_ports(2181, 2181)
        self.with_bind_ports(2888, 2888)
        self.with_bind_ports(3888, 3888)
        self.with_name("zookeper")
        self.with_kwargs(hostname="zookeper")

    def start(self, timeout=60):
        """Starts the Zookeeper and waits for it to be ready.

        Args:
            timeout (int, optional): Timeout for container to be ready. Defaults to 60.
        """
        super().start()
        wait_for_logs(self, r"binding to port", timeout=timeout)
        return self

    def get_connection_string(self):
        return f"{self.get_container_host_ip()}:{self.get_exposed_port(2181)}"

    def get_zookeeper_ip(self) -> str:
        return self.get_docker_client().bridge_ip(self._container.id)
