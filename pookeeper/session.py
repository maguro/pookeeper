from typing import Optional


class Session:
    id: Optional[int]
    last_zxid: int
    passwd: bytearray

    def __init__(self, session_id: Optional[int] = None, last_zxid: int = 0,
                 session_passwd: Optional[bytearray] = None) -> None:
        self.id = session_id
        self.last_zxid = last_zxid
        self.passwd = session_passwd if session_passwd else bytearray([0] * 16)

    def __repr__(self):
        return f"Session(id={self.id}, last_zxid={self.last_zxid})"
