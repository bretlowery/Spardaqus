import sys

from spextral.core import utils
from spextral.core.abstracts import ABCMeta, abstractmethod, abstractattribute


class SpextralIntegration:
    """
    Grandparent class for all Spextral functional classes: SpextralEndpoint, SpextralTransport, etc.
    """
    __metaclass__ = ABCMeta

    def __init__(self, integration):
        assert(integration is not None)
        assert(self.engine is not None)
        assert(self.engine.options is not None)
        assert(self.engine.options.command is not None)
        assert(self.engine.options.operation is not None)
        self.integration = integration.strip().lower()
        self.loginfo = utils.getconfig("spextral", "config", "loginfo", required=True)
        self.logerrors = utils.getconfig("spextral", "config", "logerrors", required=True)
        self.haltonerror = utils.getconfig("spextral", "config", "haltonerror", required=True)
        self.name = "SpextralGenericService"

    @abstractattribute
    def integration(self):
        pass

    @abstractattribute
    def loginfo(self):
        pass

    @abstractattribute
    def logerrors(self):
        pass

    @abstractattribute
    def haltonerror(self):
        pass

    def config(self, setting, required=False, defaultvalue=0, choices=None, intrange=None, quotestrings=False, converttolist=False):
        return utils.getconfig(self.engine.options.operation, self.integration, setting, required=required, defaultvalue=defaultvalue,
                               choices=choices, intrange=intrange, quotestrings=quotestrings, converttolist=converttolist)

    def error(self, err):
        if self.logerrors:
            utils.error('%s %s operation failed: %s' % (self.integration.capitalize(), self.engine.options.operation, err))
        if self.haltonerror:
            globals.KILLSIG = True
            sys.exit(1)
        return

    def info(self, msg):
        if self.loginfo:
            utils.info(msg)
        return


class SpextralEndpoint(SpextralIntegration):
    """
    Parent class for all Spextral endpoint classes. An endpoint in Spextral is any interface or
    adapter to an external provider that sends data to Spextral. Currently, only Splunk is supported for this.
    """
    __metaclass__ = ABCMeta

    @abstractattribute
    def bucket(self):
        pass

    @abstractattribute
    def bucketname(self):
        pass

    @abstractmethod
    def close(self, **kwargs):
        return

    @abstractmethod
    def connect(self, **kwargs):
        return

    @abstractattribute
    def connected(self):
        return

    @abstractattribute
    def connection_attempts(self):
        pass

    @abstractattribute
    def envelope(self):
        pass

    @abstractattribute
    def key(self):
        pass

    @abstractattribute
    def query(self):
        pass

    @abstractattribute
    def reader(self):
        pass

    @abstractattribute
    def servicename(self):
        pass

    @abstractattribute
    def source(self):
        pass

    @abstractattribute
    def target(self):
        pass

    @abstractattribute
    def timeout(self):
        pass

    @abstractattribute
    def totread(self):
        pass

    @abstractattribute
    def window(self):
        pass

    def __init__(self, integration):
        super().__init__(integration)
        self.bucket = "undefined"
        self.bucketname = None
        self.connection_attempts = 0
        self.envelope = {}
        self.key = None
        self.query = None
        self.reader = None
        self.source = None
        self.target = None
        self.timeout = -1
        self.totread = 0
        self.window = None
        return


class SpextralTransport(SpextralIntegration):
    """
    Parent class for all Spextral transport classes. An transporter in Spextral is any pubsub provider that
    is used for inter-Spextral communication and data transport. Currently, only Kafka is supported for this.
    """
    __metaclass__ = ABCMeta

    @abstractattribute
    def closed(self):
        return False

    @abstractmethod
    def close(self):
        return

    @abstractattribute
    def connected(self):
        return

    @abstractmethod
    def connect(self, **kwargs):
        return

    @abstractattribute
    def consumer(self):
        return

    @abstractattribute
    def consumeroptions(self):
        return

    @abstractmethod
    def getbucket(self):
        return

    @abstractattribute
    def idempotence(self):
        return

    @abstractattribute
    def json(self):
        return

    @abstractattribute
    def losstolerance(self):
        return

    @abstractattribute
    def multithread(self):
        return

    @abstractattribute
    def transportclass(self):
        return

    @abstractattribute
    def transporter(self):
        return

    @abstractattribute
    def transporteroptions(self):
        return

    @abstractattribute
    def target(self):
        return

    @abstractattribute
    def threads(self):
        return

    @abstractattribute
    def timeoutmsg(self):
        pass

    @abstractmethod
    def send(self, tuple):
        return

    def __init__(self, integration):
        super().__init__(integration)
        self.idempotence = None
        self.json = {}
        self.loss_tolerance = None
        self.multithread = None
        self.transportclass = None
        self.transporter = None
        self.transporter_options = None
        self.target = None
        self.threads = None
        self.timeoutmsg = None
        return


class SpextralAnalyzer(SpextralIntegration):
    """
        Parent class for all Spextral analyzer classes. An analyzer in Spextral is any streaming analytics provider
        used to perform DQ and other analyze work on teh data stream. Currently, only Spark is supported for this.
    """
    __metaclass__ = ABCMeta

    # @abstractattribute
    # def closed(self):
    #     return False
    #
    # @abstractmethod
    # def close(self):
    #     return
    #
    # @abstractattribute
    # def connected(self):
    #     return
    #
    @abstractmethod
    def connect(self, **kwargs):
        return
    #
    # @abstractmethod
    # def getbucket(self):
    #     return
    #
    # @abstractmethod
    # def getbuckets(self):
    #     return
    #
    # @abstractattribute
    # def idempotence(self):
    #     return
    #
    # @abstractattribute
    # def json(self):
    #     return
    #
    # @abstractattribute
    # def target(self):
    #     return

    def __init__(self, integration):
        super().__init__(integration)
        self.json = {}
        return
