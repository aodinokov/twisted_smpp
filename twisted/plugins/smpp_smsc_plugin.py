import uuid
import json

from zope.interface import implements

from twisted.python import usage, log
from twisted.plugin import IPlugin
from twisted.internet import reactor
from twisted.internet.error import ConnectionRefusedError
from twisted.internet.defer import Deferred, inlineCallbacks, returnValue
from twisted.internet.protocol import ClientCreator, ServerFactory, Protocol
from twisted.application import internet, service

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp.spec

class SmppServerNodeCtrlProtocol(AMQClient):
    def __init__(self, host, port, login, password, vhost, spec):
        self.host = host
        self.port = port
        self.login = login
        self.password = password
        self.vhost = vhost
        self.spec = spec

        AMQClient.__init__(self, 
                           delegate = TwistedDelegate(),
                           vhost = vhost,
                           spec = txamqp.spec.load(spec))
    
    @inlineCallbacks
    def _declare_queue_rx(self):
        self.queue_rx = yield self.channel_rx.queue_declare(exclusive=True, auto_delete=True)
        
    @inlineCallbacks
    def start(self, *args, **kargs):
        yield AMQClient.start(self, *args, **kargs)
        self.channel_conn = yield self.channel(0)
        self.channel_rx = yield self.channel(1)
        yield self.channel_rx.channel_open()
        #todo: it's not correct for server side
        #self.queue_rx = yield self.channel_rx.queue_declare(exclusive=True, auto_delete=True)
        yield self._declare_queue_rx()
        
        self.replies = {} # correlation_id:deferred
        yield self.channel_rx.basic_consume(queue=self.queue_rx.queue, no_ack=True, consumer_tag='qtag')
        self.queue = yield AMQClient.queue(self, 'qtag')
        self.finish_defer = Deferred()
        self._update_work_defer()
    
    def _update_work_defer(self):
        log.msg("_update_work_defer")
        self.cur_defer = self.queue.get()
        self.cur_defer.addCallbacks(self.onPacket, lambda _: None)
    
    def work(self):
        return self.finish_defer
    
    @inlineCallbacks
    def cancel(self):
        if not(self.cur_defer is None):
            yield self.channel_conn.connection_close()
            self.cur_defer.cancel()
            self.cur_defer = None
            self.finish_defer.callback(None)
    
    def onPacket(self, response):
        self._update_work_defer()

        # types of messages
        if 'reply to' in response.content.properties: # call request
            reply = Content(self.onCall(response))
            reply["delivery mode"] = 1 # non-persistent
            reply['correlation id'] = response.content.properties['correlation id']
            self.channel_rx.basic_publish(exchange = '', 
                                                routing_key = reply['reply to'],
                                                content = reply)
            return
            
        if 'correlation id' in response.content.properties: # call response
            if response.content.properties['correlation id'] in self.replies:
                self.replies[response.content.properties['correlation id']].callback(response.content.body)
            return
        
        # just async message
        self.onRecv(response)
        return

    # low level
    def onCall(self, msg):
        pass
    
    def onRecv(self, msg):
        pass
    
    @inlineCallbacks
    def call(self, exchange, routing_key, body):
        correlation_id = str(uuid.uuid4())
        
        msg = Content(body)
        msg["delivery mode"] = 1 # non-persistent
        msg['correlation id'] = correlation_id
        msg['reply to'] = self.queue_rx.queue
        
        d = Deferred()
        self.replies[correlation_id] = d
        
        try:
            yield self.channel_rx.basic_publish(exchange = exchange, 
                                                routing_key = routing_key,
                                                content = msg)
        except Exception as e:
            del self.replies[correlation_id]
            raise e
        
        timer = reactor.callLater(10, d.errback, Exception("timeout"))
        def clbck(response):
            if timer.active(): timer.cancel()
            if correlation_id in self.replies:
                del self.replies[correlation_id]
            return response
        d.addBoth(clbck)
        
        res = yield d
        returnValue(res)
    
    @inlineCallbacks
    def send(self, exchange, routing_key, body):
        msg = Content(body)
        
        msg["delivery mode"] = 1 # non-persistent
        yield self.channel_rx.basic_publish(exchange = exchange, 
                                            routing_key = routing_key,
                                            content = msg)

#class SmppServerNodeCtrlSerialize(SmppServerNodeCtrlProtocol):
#    def __init__(self, host, port, login, password, vhost, spec):
#        SmppServerNodeCtrlProtocol.__init__(self, host, port, login, password, vhost, spec)
#    
#    def call(self, exchange, routing_key, obj):
#        body = json.dumps(obj)
#        res = SmppServerNodeCtrlProtocol.call(self, exchange, routing_key, body)
#        return json.loads(res)
        

class SmppServerNode(SmppServerNodeCtrlProtocol):

    def __init__(self, host, port, login, password, vhost, spec, exchange, routing_key):
        self.exchange = exchange
        self.routing_key = routing_key
        SmppServerNodeCtrlProtocol.__init__(self, host, port, login, password, vhost, spec)


    def onRecv(self, msg):
        log.msg("received %s", msg.content.body)
        
    def onCall(self, msg):
        log.msg("called %s", msg.content.body)
        return msg.content.body

    @inlineCallbacks
    def register(self, name):
        res = yield self.call(self.exchange, self.routing_key, "register " + str(name))
        returnValue(res)
        
    @inlineCallbacks
    def unregister(self):
        res = yield self.call(self.exchange, self.routing_key, "unregister")
        returnValue(res)


class SmppServerNodeService(service.Service):
    def __init__(self):
        pass
     
    def startService(self):
        pass
    
    # put here fuctions to start/stop different services (TCPServers/SSL and etc)
    
    def stopService(self):
        pass

class SmppServerNodeCtrlService(service.Service):
    def __init__(self, node_service, host, port, login, password, vhost, exchange, routing_key, spec):
        self.node_service = node_service
        self.host = host
        self.port = port
        self.login = login
        self.password = password
        self.vhost = vhost
        self.exchange = exchange
        self.routing_key = routing_key
        self.spec = spec
         
    def startService(self):
        self.defer = self.service()
    
    @inlineCallbacks
    def service(self):
        
        creator = ClientCreator(reactor, 
                                SmppServerNode,
                                host = self.host, 
                                port = self.port,
                                login = self.login,
                                password = self.password,
                                vhost = self.vhost,
                                spec = self.spec,
                                exchange = self.exchange,
                                routing_key = self.routing_key)
        while True:
            try:
                self.protocol = yield creator.connectTCP(self.host, self.port)
                yield self.protocol.start({"LOGIN": self.login, "PASSWORD": self.password})
                log.msg("Connected")
                try:
                    yield self.protocol.register("host")
                    yield self.protocol.work()
                except Exception as e:
                    log.msg("Exception %s" %(repr(e),))
                log.msg("Disconnecting")
                yield self.protocol.cancel()
                log.msg("Disconnected")
                #break
            except ConnectionRefusedError as e:
                log.msg("Connection refused. Will try again after time-out...")
                defer = Deferred()
                timer = reactor.callLater(10, defer.callback, None)
                defer.addBoth(lambda _: timer.cancel() if timer.active() else None)
                yield defer
            except Exception as e:
                log.msg("Exception %s" %(repr(e),))
                #break

        returnValue(None)
    
    def stopService(self):
        return self.protocol.cancel()

class SmppServerNodeOptions(usage.Options):
    optParameters = [
        ['host','h', 'localhost', 'The RabbitMQ host to connect.'],
        ['port', 'p', 5672, 'The RabbitMQ port number to connect.'],
        ['vhost', None, '/', 'The RabbitMQ virtual host.'],
        ['login', None, "guest", 'The RabbitMQ login.'],
        ['password', None, "guest", 'The RabbitMQ password.'],
        ['exchange', None, "smppserv.direct", 'Exchange name to route.'],
        ['routing_key', None, "ctrl", 'Key (e.g. queue name) to route.'],
        ['spec', None, "amqp0-8.stripped.rabbitmq.xml", 'Spec file.'],
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

        node_ctrl_service = SmppServerNodeCtrlService(node_service = node_service, 
                                                      host = options['host'], 
                                                      port = int(options['port']), 
                                                      login = options['login'], 
                                                      password = options['password'], 
                                                      vhost = options['vhost'],
                                                      exchange = options['exchange'], 
                                                      routing_key = options['routing_key'],
                                                      spec = options['spec'])
        node_ctrl_service.setServiceParent(top_service)

        return top_service

service_maker = SmppServerNodeServiceMaker()
