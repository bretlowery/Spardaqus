import redis
from redis.exceptions import ConnectionError
from spardaqus.core.utils import getconfig, error


class Redis:
    def __init__(self):
        self.name = "SpardaqusRedisStateManager"
        self.host = getconfig("spardaqus", "redis", "host", required=True)
        self.port = getconfig("spardaqus", "redis", "port", required=True, defaultvalue=6379, intrange=[1, 65535])
        self.db = getconfig("spardaqus", "redis", "db", required=True, defaultvalue=0, intrange=[0, 9999])
        self.connection = redis.Redis(host=self.host, port=self.port, db=self.db)
        self.pipeline = self.connection.pipeline()

    def connectionerror(self, ce):
        error("State control failed; connection refused or dropped to Redis at %s:%d: %s" % (self.host, self.post, str(ce)))

    def set(self, key, val):
        """Write a value to a Redis key."""
        if val is None:
            val = "none"
        try:
            self.pipeline.set(key, str(val.strip()).encode('utf-8'))
        except ConnectionError as ce:
            self.connectionerror(ce)

    def commit(self):
        """Commit a Redis transaction."""
        try:
            return self.pipeline.execute()
        except ConnectionError as ce:
            self.connectionerror(ce)

    def get(self, key):
        """Read a value from a Redis key."""
        v = ""
        try:
            v = self.connection.get(key)
        except ConnectionError as ce:
            self.connectionerror(ce)
        if not v:
            return None
        elif v.lower() == "none":
            return None
        else:
            return v.decode('utf-8')

    def delete(self, key):
        """Delete a Redis KV pair by key."""
        try:
            self.connection.delete(key)
        except ConnectionError as ce:
            self.connectionerror(ce)

