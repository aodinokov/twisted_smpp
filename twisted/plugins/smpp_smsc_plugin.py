from zope.interface import implements

from twisted.python import usage, log
from twisted.plugin import IPlugin
from twisted.internet import reactor
from twisted.internet.protocol import ServerFactory, Protocol
from twisted.internet.defer import Deferred, inlineCallbacks, returnValue
from twisted.internet.protocol import ClientCreator, ServerFactory, Protocol
from twisted.application import internet, service

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp.spec


class SmppServerNodeService(service.Service):
    def __init__(self):
        pass
     
    def startService(self):
        pass
    
    # put here fuctions to start/stop different services (TCPServers/SSL and etc)
    
    def stopService(self):
        pass

class SmppServerNodeCtrlService(service.Service):
    def __init__(self, service, host, port, login, password, vhost,
                 exchange, routing_key):
        self.service = service
        self.host = host
        self.port = port
        self.login = login
        self.password = password
        self.vhost = vhost
        self.exchange = exchange
        self.routing_key = routing_key
         
    def startService(self):
        self.defer = self.serviceWork()
    
    @inlineCallbacks
    def send(self, channel, body):
        msg = Content(body)
        msg["delivery mode"] = 1 # non-persistent
        yield channel.basic_publish(exchange = self.exchange, 
                                    routing_key = self.routing_key,
                                    content = msg)
    
    @inlineCallbacks
    def serviceWork(self):
        delegate = TwistedDelegate()
        creator = ClientCreator(reactor, 
                                AMQClient, 
                                delegate = delegate, 
                                vhost=self.vhost)
        self.protocol = yield creator.connectTCP(self.host, self.port)
        yield self.protocol.start({"LOGIN": self.login, "PASSWORD": self.password})
        channelRx = yield self.protocol.channel(0)
        channelTx = yield self.protocol.channel(1)
        try:
            yield channelRx.queue_declare(queue='', 
                                          durable=False,
                                          exclusive=True,
                                          auto_delete=True)
            # register on server
            self.send(channelTx, "test")
            # TODO: consume messages
        except Exception as e:
            pass
        self.protocol = None
        yield channelRx.connection_close()
        returnValue(None)
    
    def stopService(self):
        return self.defer.cancel()

class SmppServerNodeOptions(usage.Options):
    optParameters = [
        ['host','h', 'localhost', 'The RabbitMQ host to connect.'],
        ['port', 'p', 5672, 'The RabbitMQ port number to connect.'],
        ['vhost', None, '/', 'The RabbitMQ virtual host.'],
        ['login', None, "guest", 'The RabbitMQ login.'],
        ['password', None, "guest", 'The RabbitMQ password.'],
        ['exchange', None, "smppserv.direct", 'Exchange name to route.'],
        ['routing_key', None, "ctrl", 'Key (e.g. queue name) to route.'],
        ]



class SmppServerNodeServiceMaker(object):

    implements(service.IServiceMaker, IPlugin)

    tapname = "smpp-serv-node"
    description = "A SMPP server worker node."
    options = SmppServerNodeOptions

    def makeService(self, options):
        top_service = service.MultiService()

        node_service = SmppServerNodeService()
        node_service.setServiceParent(top_service)

        node_ctrl_service = SmppServerNodeCtrlService(service = node_service, 
                                                      host = options['host'], 
                                                      port = int(options['port']), 
                                                      login = options['login'], 
                                                      password = options['password'], 
                                                      vhost = options['vhost'],
                                                      exchange = options['exchange'], 
                                                      routing_key = options['routing_key'])
        node_ctrl_service.setServiceParent(top_service)

        return top_service

service_maker = SmppServerNodeServiceMaker()
