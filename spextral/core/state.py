import redis
from spextral.core.utils import getconfig


class Redis:
    def __init__(self):
        self.name = "SpextralRedisStateManager"
        self.host = getconfig("spextral", "redis", "host", required=True)
        self.port = getconfig("spextral", "redis", "port", required=True, defaultvalue=6379, intrange=[1, 65535])
        self.db = getconfig("spextral", "redis", "db", required=True, defaultvalue=0, intrange=[0, 9999])
        self.connection = redis.Redis(host=self.host, port=self.port, db=self.db)
        self.pipeline = self.connection.pipeline()

    def set(self, key, val):
        """Write a value to a Redis key."""
        if val is None:
            val = "none"
        self.pipeline.set(key, str(val.strip()).encode('utf-8'))

    def commit(self):
        """Commit a Redis transaction."""
        return self.pipeline.execute()

    def get(self, key):
        """Read a value from a Redis key."""
        v = self.connection.get(key)
        if not v:
            return None
        elif v.lower() == "none":
            return None
        else:
            return v.decode('utf-8')

    def delete(self, key):
        """Delete a Redis KV pair by key."""
        self.connection.delete(key)

