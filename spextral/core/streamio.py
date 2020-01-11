import io


class SpextralStreamBuffer(io.RawIOBase):
    """ Super-basic buffer logic to improve streaming data performance. """
    def __init__(self, reader):
        self.reader = reader

    def readable(self):
        return True

    def close(self):
        self.reader.close()

    def read(self, n):
        return self.reader.read(n)

    def readinto(self, b):
        sz = len(b)
        data = self.reader.read(sz)
        for idx, ch in enumerate(data):
            b[idx] = ch
        return len(data)

