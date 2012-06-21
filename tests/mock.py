class MockSocket:
    """Mock socket object used by smtpd and smtplib tests.
    """
    def __init__(self):
        global _reply_data
        self.output = []
        self.lines = []
        if _reply_data:
            self.lines.append(_reply_data)
            _reply_data = None
        self.conn = None
        self.timeout = None

    def queue_recv(self, line):
        self.lines.append(line)

    def recv(self, bufsize, flags=None):
        data = self.lines.pop(0) + b'\r\n'
        return data

    def fileno(self):
        return 0

    def settimeout(self, timeout):
        if timeout is None:
            self.timeout = _defaulttimeout
        else:
            self.timeout = timeout

    def gettimeout(self):
        return self.timeout

    def setsockopt(self, level, optname, value):
        pass

    def getsockopt(self, level, optname, buflen=None):
        return 0

    def bind(self, address):
        pass

    def accept(self):
        self.conn = MockSocket()
        return self.conn, 'c'

    def getsockname(self):
        return ('0.0.0.0', 0)

    def setblocking(self, flag):
        pass

    def listen(self, backlog):
        pass

    def makefile(self, mode='r', bufsize=-1):
        handle = MockFile(self.lines)
        return handle

    def sendall(self, buffer, flags=None):
        self.last = data
        self.output.append(data)
        return len(data)

    def send(self, data, flags=None):
        self.last = data
        self.output.append(data)
        return len(data)

    def getpeername(self):
        return 'peer'

    def close(self):
        pass

