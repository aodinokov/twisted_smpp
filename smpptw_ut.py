
from smpptw import *

from twisted.internet.protocol import ClientCreator, ServerFactory
from twisted.trial.unittest import TestCase
from twisted.internet import reactor
from twisted.internet.defer import Deferred, inlineCallbacks, returnValue

class SmppGenericUt(SmppGeneric):
    def connectionMade(self):
        SmppGeneric.connectionMade(self)
        self.factory.protocolConnectionMade(self)
    def connectionLost(self, reason):
        self.factory.protocolConnectionLost(self, reason)
        SmppGeneric.connectionLost(self, reason)
    def unexpectedError(self, reason):
        self.factory.protocolUnexpectedError(self, reason)
        SmppGeneric.unexpectedError(self, reason)
    def smppValuablePduReceived(self, pdu):
        self.factory.protocolValuablePduReceived(self, pdu)
        SmppGeneric.smppValuablePduReceived(self, pdu)
    def smppRespPduReceived(self, pdu):
        self.factory.protocolRespPduReceived(self, pdu)
        SmppGeneric.smppRespPduReceived(self, pdu)

class SmppEcmeUt(SmppGeneric):
    smppTuneEsme()
class SmppSmscUt(SmppGenericUt):
    smppTuneSmsc()

class SmppServerFactoryUt(ServerFactory):
    protocol = SmppSmscUt
    
    def __init__(self,
                 protocolConnectionMadeCallback = None, protocolConnectionMadeCallbackKArgs = {},
                 protocolConnectionLostCallback = None, protocolConnectionLostCallbackKArgs = {},
                 protocolUnexpectedErrorCallback = None, protocolUnexpectedErrorCallbackKArgs = {},
                 protocolIcorrectPduCommandIdCallback = None, protocolIcorrectPduCommandIdCallbackKArgs = {},
                 protocolValuablePduReceivedCallback = None, protocolValuablePduReceivedCallbackKArgs = {},
                 protocolRespPduReceivedCallback = None, protocolRespPduReceivedCallbackKArgs = {}
                 ):
        self.protocolConnectionMadeCallback = protocolConnectionMadeCallback
        self.protocolConnectionMadeCallbackKArgs = protocolConnectionMadeCallbackKArgs
        self.protocolConnectionLostCallback = protocolConnectionLostCallback
        self.protocolConnectionLostCallbackKArgs = protocolConnectionLostCallbackKArgs
        self.protocolUnexpectedErrorCallback = protocolUnexpectedErrorCallback
        self.protocolUnexpectedErrorCallbackKArgs = protocolUnexpectedErrorCallbackKArgs
        self.protocolIcorrectPduCommandIdCallback = protocolIcorrectPduCommandIdCallback
        self.protocolIcorrectPduCommandIdCallbackKArgs = protocolIcorrectPduCommandIdCallbackKArgs
        self.protocolValuablePduReceivedCallback = protocolValuablePduReceivedCallback
        self.protocolValuablePduReceivedCallbackKArgs = protocolValuablePduReceivedCallbackKArgs
        self.protocolRespPduReceivedCallback = protocolRespPduReceivedCallback
        self.protocolRespPduReceivedCallbackKArgs = protocolRespPduReceivedCallbackKArgs

    def protocolConnectionMade(self, protocol):
        if self.protocolConnectionMadeCallback:
            self.protocolConnectionMadeCallback(protocol, **self.protocolConnectionMadeCallbackKArgs)    
    def protocolConnectionLost(self, protocol, reason):
        if self.protocolConnectionLostCallback:
            self.protocolConnectionLostCallback(protocol, reason, **self.protocolConnectionLostCallbackKArgs)
    def protocolUnexpectedError(self, protocol, reason):
        if self.protocolUnexpectedErrorCallback:
            self.protocolUnexpectedErrorCallback(protocol, reason, **self.protocolUnexpectedErrorCallbackKArgs)
    def protocolValuablePduReceived(self, protocol, pdu):
        if self.protocolValuablePduReceivedCallback:
            self.protocolValuablePduReceivedCallback(protocol, pdu, **self.protocolValuablePduReceivedCallbackKArgs)
    def protocolRespPduReceived(self, protocol, pdu):
        if self.protocolRespPduReceivedCallback:
            self.protocolRespPduReceivedCallback(protocol, pdu, **self.protocolRespPduReceivedCallbackKArgs)

class SmppTest(TestCase):
    
    def _serv_protocol(self, protocol):
        #self.serverProtocols[protocol.transport.getHost().port] = protocol
        pass
    
    def _serv_error(self, *args, **kargs):
        log.msg("serv_error args %s kargs %s" % (repr(args), repr(kargs)))
        #todo: remove protocol
        
    def _serv_pdu(self, protocol, pdu, type):
        log.msg("serv_pdu type %s protocol %s pdu %s" % (type, repr(protocol), repr(pdu)))
    
    def setUp(self):
        self.serverFactory = SmppServerFactoryUt(
                                               protocolConnectionMadeCallback = self._serv_protocol,
                                               protocolConnectionLostCallback = self._serv_error,
                                               protocolConnectionLostCallbackKArgs = {"error":"connectionLost"},
                                               protocolUnexpectedErrorCallback = self._serv_error,
                                               protocolUnexpectedErrorCallbackKArgs = {"error":"cantParseInput"},
                                               protocolValuablePduReceivedCallback = self._serv_pdu,
                                               protocolValuablePduReceivedCallbackKArgs = {"type":"valuable"},
                                               protocolRespPduReceivedCallback = self._serv_pdu,
                                               protocolRespPduReceivedCallbackKArgs = {"type":"resp"}
                                               )
        self.serverProtocols = {}
        self.port = reactor.listenTCP(0, self.serverFactory, interface="127.0.0.1")
        self.portnum = self.port.getHost().port
        log.msg("setUp finished")
    
    def tearDown(self):
        port, self.port = self.port, None
        self.serverFactory = None
        return port.stopListening()
    
    @inlineCallbacks
    def test_smoke_1(self):
        creator = ClientCreator(reactor, SmppEcmeUt)
        protocol = yield creator.connectTCP("127.0.0.1", self.portnum)
        pdu = BindTransmitter(1).obj
        log.msg("created pdu %s" % (repr(pdu),))
        pdu = yield protocol.smmpValuablePduSendAndWaitResp(pdu)
        log.msg("got pdu %s" % (repr(pdu),))
        pdu = yield protocol.unbind()
        log.msg("unbinded")
        returnValue(pdu)
