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
import logging
import threading
from queue import Queue
from typing import Callable, Optional

LOGGER = logging.getLogger(__name__)


class Events:
    _event_thread_completed:threading.Event
    _event_thread: threading.Thread

    def __init__(self, client_id: int):
        self.client_id = client_id
        self._events = Queue()

    def start(self):
        self._event_thread_completed = threading.Event()

        def event_worker():
            try:
                while True:
                    notification = self._events.get()

                    if notification == self:
                        break

                    try:
                        notification()

                    except Exception as e:
                        LOGGER.exception(f"Unforeseen error during notification: {str(e)}")

            finally:
                LOGGER.debug('Event loop completed')
                self._event_thread_completed.set()

        self._event_thread = threading.Thread(target=event_worker, name=f"pookeeper-event-{self.client_id}")
        self._event_thread.daemon = True
        self._event_thread.start()

    def stop(self):
        self._events.put(self)
        self._event_thread_completed.wait()

    def join(self, timeout: Optional[float] = None) -> bool:
        """Block until the event thread completes.

        If the event thread completed on entry, return immediately. Otherwise,
        block until the event thread completes, or until the optional timeout occurs.

        When the timeout argument is present and not None, it should be a
        floating-point number specifying a timeout for the operation in seconds
        (or fractions thereof).

        This method returns the event thread completion status on exit, so it will
        always return True except if a timeout is given and the operation times out.

        """
        return self._event_thread_completed.wait(timeout)

    def put(self, item: Callable, block: bool = True, timeout: int = None):
        """Put an event notification into the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until a free slot is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Full exception if no free slot was available within that time.
        Otherwise ('block' is false), put an item on the queue if a free slot
        is immediately available, else raise the Full exception ('timeout'
        is ignored in that case).
        """
        self._events.put(item, block, timeout)
