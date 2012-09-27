import json
import uuid

from smpptw import *

from twisted.internet.protocol import ClientCreator, ServerFactory
from twisted.internet.defer import Deferred, succeed, fail, inlineCallbacks, returnValue
from twisted.internet.error import ConnectionClosed
from twisted.python.failure import Failure
from twisted.application import service


from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp.spec

class SmppSmsc(SmppGeneric):
    smppTuneSmsc()
    def connectionMade(self):
        SmppGeneric.connectionMade(self)
        self.factory.protocolEvent(self, ("connectionMade",))
    def connectionLost(self, reason):
        self.factory.protocolEvent(self, ("connectionLost", reason,))
        SmppGeneric.connectionLost(self, reason)
    def unexpectedError(self, reason):
        self.factory.protocolEvent(self, ("unexpectedError", reason,))
    def onBindPdu(self, pdu):
        self.factory.protocolEvent(self, ("bind", pdu))
    def onUnbindPdu(self, pdu):
        self.factory.protocolEvent(self, ("unbind", pdu,))
    def onOtherPdu(self, pdu):
        self.factory.protocolEvent(self, ("otherPdu", pdu,))

class SmppSmscFactory(ServerFactory):
    protocol = SmppSmsc
    def __init__(self, protocolEventCallback = None, protocolEventCallbackKArgs = {}):
        self.protocolEventCallback = protocolEventCallback
        self.protocolEventCallbackKArgs = protocolEventCallbackKArgs
    
    def protocolEvent(self, protocol, args):
        if self.protocolEventCallback:
            self.protocolEventCallback(protocol, args, **self.protocolEventCallbackKArgs)

class SmscServer(service.Service):

    # todo - move it somethere? as supported_listen_proto 
    from twisted.internet import reactor
    supported_proto = {
             "TCP": reactor.listenTCP,
             "SSL": reactor.listenSSL,
            }

    def __init__(self,
                 smsc_service,
                 smpp_listen_proto,
                 smpp_listen_kargs):
        self.smsc_service = smsc_service
        self.smpp_listen_proto = smpp_listen_proto
        self.smpp_listen_kargs = smpp_listen_kargs
    
    def startService(self):
        kargs = {"server": self}
        self.smpp_listen_kargs['factory'] = SmppSmscFactory(protocolEventCallback = self.smsc_service._on_smpp_protocol_event,
                                                            protocolEventCallbackKArgs = kargs)
        
        if not (self.smpp_listen_proto in self.supported_proto):
            raise Exception("Unsupported smpp_listen_proto {}".format(self.smpp_listen_proto))
            
        self.port = self.supported_proto[self.smpp_listen_proto](**self.smpp_listen_kargs)            
    
    def stopService(self):
        port, self.port = self.port, None
        return port.stopListening()

class SmscService(service.Service):
    
    default_call_timeout = 5
    
    def __init__(self,
                 amqp_connect_proto, 
                 amqp_connect_kargs,
                 amqp_vhost,
                 amqp_spec_path,
                 amqp_start_args,
                 work_args):
        self.amqp_connect_proto = amqp_connect_proto 
        self.amqp_connect_kargs = amqp_connect_kargs
        self.amqp_vhost = amqp_vhost
        self.amqp_spec_path = amqp_spec_path
        self.amqp_start_args = amqp_start_args
        self.work_args = work_args
    
    def _correlation_dict_init(self):
        self.replies = {}
        
    def _correlation_dict_add(self, correlation_id, deferred):
        self.replies[correlation_id] = deferred
        
    def _correlation_callback(self, correlation_id, content):
        if correlation_id in self.replies: 
            d = self.replies[correlation_id]
            d.callback(content)
    
    @inlineCallbacks
    def _call(self, exchange, routing_key, content, timeout = None):
        assert (not (content['correlation id'] is None)), "'correlation id' is not specified"
        assert (not (content['reply to'] is None)), "'reply to' is not specified"
        
        correlation_id = content['correlation id']
        d = Deferred()
        self.replies[correlation_id] = d

        timeout = timeout or self.default_call_timeout
        timer = reactor.callLater(timeout, d.errback, Exception("timeout"))
        try:
            yield self.channel.basic_publish(exchange = self.work_args['auth']['exchange'],
                                             routing_key = self.work_args['auth']['routing_key'],
                                             content = content)
            x = yield d
            returnValue(x)
        except Exception as e:
            raise e
        finally:
            del self.replies[correlation_id]
            if timer.active(): 
                timer.cancel()

    @inlineCallbacks
    def _call_wrp(self, exchange, routing_key, msg, timeout = None):
        content = Content(msg)
        content['delivery mode'] = 1 # non-persistent
        content['correlation id'] = str(uuid.uuid4())
        content['reply to'] = self.r_queue.queue
        resp = yield self._call(exchange, routing_key, content, timeout)
        returnValue(resp.content.body)

    def _on_amqp_msg(self, msg):
        # continue to consume
        self._update_queue_defer()
        # check if it's our response
        if 'correlation id' in msg.content.properties:
            self._correlation_callback(msg.content.properties['correlation id'], msg)
            return succeed(None)
        return self._on_amqp_income_msg(msg)
    
    def _on_smpp_protocol_event(self, protocol, args, server):
        if args[0] == "bind":
            return self._process_smmp_bind(protocol, args[1])
        if args[0] == "unbind":
            return self._process_smmp_unbind(protocol, args[1])
        if args[0] == "otherPdu":
            return self._process_smmp_otherPdu(protocol, args[1])
    
    def _on_amqp_income_msg(self, msg):
        pass

    def _on_amqp_err(self, failure):
        #TODO: if connection is lost - it could be a good place to detect that
        pass
    
    @inlineCallbacks
    def _process_smmp_bind(self, protocol, pdu):
        log.msg("!!!bind %s" % (repr(pdu)))
        status = 'ESME_ROK'        
        try:
            res = yield self._call_wrp(exchange = self.work_args['auth']['exchange'], routing_key = self.work_args['auth']['routing_key'], msg = json.dumps(pdu))
            x = json.loads(res) # just as example!
            if x in None:
                status = 'ESME_RINVPASWD'
        except Exception as e:
            log.msg("exception %s" % (repr(e)))
            status = 'ESME_RSYSERR'
        
        # add protocol to route table and so on...
        if status == 'ESME_ROK':
            pass

        #send result
        pdu_resp = BindResp(pdu['header']['command_id']+'_resp', status, pdu['header']['sequence_number']).obj 
        yield protocol.bindResp(pdu, pdu_resp)

    @inlineCallbacks
    def _process_smmp_unbind(self, protocol, pdu):
        log.msg("!!!unbind %s" % (repr(pdu)))        
        # remove protocol from route table and so on...
#        try:
#            yield self._call(exchange = self.work_args['auth']['exchange'], routing_key = self.work_args['auth']['routing_key'], msg = json.dumps(pdu))
#        except Exception as _:
#            pass
        #send OK result
        yield protocol.unbindResp(pdu)

    @inlineCallbacks
    def _process_smmp_otherPdu(self, protocol, pdu):        
        # remove protocol from route table and so on...
        try:
            yield self._call(exchange = self.work_args['auth']['exchange'], routing_key = self.work_args['auth']['routing_key'], msg = json.dumps(pdu))
        except Exception as _:
            pass
        #TODO: send some result anyway
        #yield protocol.unbindResp(pdu)

    def _update_queue_defer(self):
        self.queue_defer = self.queue.get()
        self.queue_defer.addCallbacks(self._on_amqp_msg, self._on_amqp_err) 
    
    @inlineCallbacks
    def startService(self):
        from twisted.internet import reactor
        
        creator = ClientCreator(reactor, AMQClient, delegate = TwistedDelegate(), 
                                vhost = self.amqp_vhost, spec = txamqp.spec.load(self.amqp_spec_path))
        supported_proto = {
                 "TCP": creator.connectTCP,
                 "SSL": creator.connectSSL,
                }
        if not (self.amqp_connect_proto in supported_proto):
            raise Exception("Unsupported amqp_connect_proto {}".format(self.amqp_connect_proto))

        #TODO: make loop to try reconnect on failure
        self._correlation_dict_init()
        # connect
        self.connection = yield supported_proto[self.amqp_connect_proto](**self.amqp_connect_kargs)
        # authenticate
        yield self.connection.start(self.amqp_start_args)
        # open channel
        self.channel = yield self.connection.channel(1)
        yield self.channel.channel_open()
        # declare exclusive queue
        self.r_queue = yield self.channel.queue_declare(exclusive=True, auto_delete=True)
        # setup queue to consume
        yield self.channel.basic_consume(queue=self.r_queue.queue, no_ack=True, consumer_tag='cmds')
        self.queue = yield self.connection.queue('cmds')
        
        # start consume
        self._update_queue_defer()
        returnValue(None)

    @inlineCallbacks
    def stopService(self):
        # TODO: stop all protocols gracefully (with unbind)
        self.queue_defer, d = None, self.queue_defer  
        if d:
            d.cancel()
        self.channel = None
        self.connection, connection = None, self.connection
        if connection:
            chan0 = yield connection.channel(0)
            yield chan0.connection_close()
        returnValue(None)

work_args = {
                "auth":{
                        'exchange':'',
                        'routing_key':'',
                        },
            }

application = service.Application("smsc")
smsc_service = SmscService("TCP", {"host":"127.0.0.1", "port": 5672}, "/", 
                           "/home/mralex/workspace/clickatell/src/amqp0-8.stripped.rabbitmq.xml", 
                           {"LOGIN": "guest", "PASSWORD": "guest"}, work_args)
smsc_service.setServiceParent(application)

smsc_server1 = SmscServer(smsc_service, "TCP", {"interface":"127.0.0.1", "port":10000})
smsc_server1.setServiceParent(application)

smsc_server2 = SmscServer(smsc_service, "TCP", {"interface":"127.0.0.1", "port":10001})
smsc_server2.setServiceParent(application)



