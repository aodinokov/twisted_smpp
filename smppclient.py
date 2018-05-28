
from smpptw import *

from twisted.internet.protocol import ClientCreator
from twisted.internet import reactor
from twisted.internet.defer import Deferred, succeed, fail, inlineCallbacks, returnValue

class SmppEcme(SmppGeneric):
    smppTuneEsme()

@inlineCallbacks
def main():
    creator = ClientCreator(reactor, SmppEcme)
    protocol = yield creator.connectTCP("127.0.0.1", 10000)
    pdu = BindTransmitter(1).obj
    #log.msg("created pdu %s" % (repr(pdu),))
    print "created pdu ", pdu
    try:
        pdu = yield protocol.bind(pdu)
        print "got pdu ", pdu
    except Exception as e:
        print e
    #log.msg("got pdu %s" % (repr(pdu),))
    pdu = yield protocol.unbind()
    print "got pdu ", pdu
    #log.msg("unbinded")
    returnValue(pdu)

d = main()
def err(err):
    #log.msg("Error %s" % (err,))
    print err
    return err
d.addErrback(err)
d.addBoth(lambda res : reactor.stop())
reactor.run()
