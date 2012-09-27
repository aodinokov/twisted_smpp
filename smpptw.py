
from smpp.pdu_builder import *

from twisted.internet import reactor
from twisted.internet.protocol import Protocol
from twisted.internet.defer import Deferred, succeed, fail
from twisted.python.failure import Failure
from twisted.python import log

# todo raname to txsmpp
#TO Fix: 
#1. only version 34
#!!!fixed!!! 2. internal packet filter can fail(incorrect current state) 
#!!!fixed!!! 3. problems with error handling during session (we need one function with Failure instead of many)
#!!!fixed!!! 4. client and server factories are too complicated
#!!!fixed(check one place now)!!!5. probably we have some problems with sequence_number
#6. Pdu_builder is not fit very much
#!!!fixed!!!7. add default timeout
#7. We should filter some errors like Message Length is invalid, Invalid Command ID and so on and return result automatically
#]


class SmppGeneric(Protocol):
    default_resp_timeout = 10

    def connectionMade(self):
        self.defer={}
        self.waiting_valuable={}
        self.waiting_resp={}
        
        self.data = ''        
        self._newState('OPEN')
        
        self.sequence_number_tx = 0
        self.sequence_number_tx_resp = 0
        self.sequence_number_rx = 0
        self.sequence_number_rx_resp = 0
        # todo: not implemented
        self.interface_version = 34

    def dataReceived(self, data):
        log.msg("data is received %s" % (repr(data),))
        self.data += data
        if len(self.data) < 4:
            return
        pdu_len = int(binascii.b2a_hex(self.data[0:4]), 16)
        #TODO: check pdu_len (it's possible to 0xff^4 and we get 4Gb of mem - it's not correct)
        if len(self.data) < pdu_len:
            log.msg("data is not enought %s < %s", (str(len(self.data)), str(pdu_len)))
            return
        cur_data=self.data[0:pdu_len]
        self.data = self.data[pdu_len:]
        log.msg("rest of the data %s" % (repr(self.data),))
        #TODO: what version of SMPP protocol?
        pdu = unpack_pdu(cur_data)
        if pdu == None:
            log.msg("can't parse SMPP")
            self.unexpectedError(Failure(Exception("can't parse SMPP {}".format(repr(cur_data)))))
        else: 
            log.msg("got SMPP PDU")
            self.smppPduReceived(pdu)

    # low level interface    
    def smmpPduSend(self, pdu):        
        self.transport.write(pack_pdu(pdu))

    def smppPduReceived(self, pdu):
        if not (self._canBeIssuedByPeer(pdu)):
            self.unexpectedError(Failure(Exception("Incorrect input Pdu Id {}".format(repr(pdu)))))
            return
        sequence_number = int(pdu['header']['sequence_number'])
        if not SmppGeneric._isRespPdu(pdu):
            if not (sequence_number == SmppGeneric._nextSequenceNumber(self.sequence_number_rx)):
                log.msg("incorrect sequence_number")
                self.unexpectedError(Failure(Exception("Incorrect input sequence_number {}".format(sequence_number))))
            else:
                self.sequence_number_rx = sequence_number 
                self.smppValuablePduReceived(pdu)
        else:
            if not (sequence_number == SmppGeneric._nextSequenceNumber(self.sequence_number_tx_resp)):
                log.msg("incorrect sequence_number")
                self.unexpectedError(Failure(Exception("Incorrect input response sequence_number {}".format(sequence_number))))
            else:
                self.sequence_number_tx_resp = sequence_number
                if sequence_number in self.waiting_valuable:
                    d = self.waiting_valuable[sequence_number][1]
                    #del self.waiting_valuable[sequence_number]
                    d.callback(pdu)
                self.smppRespPduReceived(pdu)

    # mid level interface
    def smmpValuablePduSend(self, pdu):
        log.msg("smmpValuablePduSend")
        if SmppGeneric._isRespPdu(pdu): raise Exception("PDU can't be resp")
        if not (self._canBeIssuedByHost(pdu)): raise Exception("PDU can't be sent by host")
        pdu['header']['sequence_number'] = SmppGeneric._nextSequenceNumber(self.sequence_number_tx)
        try:
            self.smmpPduSend(pdu)
            self.sequence_number_tx = SmppGeneric._nextSequenceNumber(self.sequence_number_tx)
        except Exception as e:
            raise e

    def smmpValuablePduSendAndWaitResp(self, pdu):
        log.msg("smmpValuablePduSendAndWaitResp")
        if SmppGeneric._isRespPdu(pdu): raise Exception("PDU can't be resp")
        if not (self._canBeIssuedByHost(pdu)): raise Exception("PDU can't be sent by the host")
        d = Deferred()
        try:
            self.smmpValuablePduSend(pdu)
        except Exception as e:
            return fail(e)
        self.waiting_valuable[self.sequence_number_tx] = (pdu, d)
        def onRemoveDef(x):
            if self.sequence_number_tx in self.waiting_valuable: 
                del self.waiting_valuable[self.sequence_number_tx]
            return x 
        d.addBoth(onRemoveDef)
        return d
        
    def smmpRespPduSend(self, pdu):
        log.msg("smmpRespPduSend")
        if not SmppGeneric._isRespPdu(pdu): raise Exception("PDU has to be resp")
        if not (self._canBeIssuedByHost(pdu)): raise Exception("PDU can't be sent by host")
        if int(pdu['header']['sequence_number']) == SmppGeneric._nextSequenceNumber(self.sequence_number_rx_resp):
            log.msg("sending SMPP PDU resp")
            try:
                self.smmpPduSend(pdu)
                self.sequence_number_rx_resp = SmppGeneric._nextSequenceNumber(self.sequence_number_rx_resp)
                res = succeed(pdu)
            except Exception as e:
                res = fail(e)
            while SmppGeneric._nextSequenceNumber(self.sequence_number_rx_resp) in self.waiting_resp:
                log.msg("sending waiting SMPP PDU resp")
                (lpdu, d) = self.waiting_resp[SmppGeneric._nextSequenceNumber(self.sequence_number_rx_resp)]
                del self.waiting_resp[SmppGeneric._nextSequenceNumber(self.sequence_number_rx_resp)]
                try:
                    self.smmpPduSend(lpdu)
                    self.sequence_number_rx_resp = SmppGeneric._nextSequenceNumber(self.sequence_number_rx_resp)
                    d.callback(lpdu)
                except Exception as e:
                    d.errback(e)
            return res
        log.msg("queue-ing SMPP PDU resp")
        d = Deferred()
        self.waiting_resp[pdu['header']['sequence_number']] = (pdu, d)
        return d 

    def smppValuablePduReceived(self, pdu):
        if pdu['header']['command_id'] in [
                'bind_transmitter',
                'bind_receiver',
                'bind_transceiver']:
            return self.onBindPdu(pdu)
        if pdu['header']['command_id'] in ['unbind']:
            return self.onUnbindPdu(pdu)
        return self.onOtherPdu(pdu)

    def smppRespPduReceived(self, pdu):
        pass

    # high level interface
    def unexpectedError(self, reason):
        """ It's possible to get here all unexpected input errors
        """
        log.msg("connectionLost arg %s" % (reason,))
        self.loseConnection()
    
    def connectionLost(self, reason):
        """ forced connection loss (without unbind) from peer side
        """
        log.msg("connectionLost arg %s" % (reason,))
        if self.state != 'CLOSED':
            log.msg("unexpected connection loss")
            self._newState('CLOSED')
        
        self._errbackAllWaiting(reason)

    def loseConnection(self, arg=None):
        """ forced connection loss (without unbind)
        """
        log.msg("loseConnection arg %s" % (repr(arg)))
        self._newState('CLOSED')
        self.transport.loseConnection(arg)
        return arg

    def unbind(self, pdu=None):
        """ soft protocol shutdown
        if pdu is None - default unbind is used
        """
        log.msg("unbind pdu %s" % (repr(pdu),))
        if pdu is None: 
            pdu = Unbind(self.sequence_number_tx).obj
        d = self.smmpValuablePduSendAndWaitResp(pdu)
        
        timer = reactor.callLater(self.default_resp_timeout, d.errback, Exception("timeout"))

        def onError(reason):
            if timer.active(): timer.cancel()
            return None
        def onResp(pdu_resp):
            if timer.active(): timer.cancel()
            return pdu_resp

        d.addCallbacks(onResp, onError)
        d.addBoth(self.loseConnection)
        return d

    def onUnbindPdu(self, pdu):
        """ 
        redefine this method to add extra handler
        or redefine it at all if you want to make sending responce by defer 
        """
        log.msg("onUnbindPdu pdu %s" % (repr(pdu),))
        self.unbindResp(pdu)
    
    def unbindResp(self, pdu, pdu_resp=None):
        """
        use this function to send response, 
        because it correctly changes connection state
        """
        if pdu_resp is None:
            pdu_resp = PDU('unbind_resp', "ESME_ROK", pdu['header']['sequence_number']).obj
        if pdu['header']['sequence_number'] != pdu_resp['header']['sequence_number']:
            raise Exception("invalid resp sequence_number")                         
        d = self.smmpRespPduSend(pdu_resp)
        d.addBoth(self.transport.loseConnection)

    def bind(self, pdu):
        """
        bind. pdu should contain all necessary info to bind except sequence_number
        """
        log.msg("bind pdu %s" % (repr(pdu),))
        d = self.smmpValuablePduSendAndWaitResp(pdu)
        timer = reactor.callLater(self.default_resp_timeout, d.errback, Exception("timeout"))

        def onError(reason):
            if timer.active(): timer.cancel()
            self.loseConnection(reason)
            return reason
        def onResp(pdu_resp):
            if timer.active(): timer.cancel()
            if pdu['header']['command_id']+"_resp" != pdu_resp['header']['command_id']: 
                raise Exception("invalid response")
            return pdu_resp

        d.addCallback(onResp)
        d.addErrback(onError)
        return d

    def onBindPdu(self, pdu):
        """
        redefine it
        """
        log.msg("onUnbindPdu pdu %s" % (repr(pdu),))
        self.bindResp(pdu)

    def bindResp(self, pdu, pdu_resp=None):
        """
        use this function to send response, 
        because it correctly changes connection state
        """
        if pdu_resp is None:
            pdu_resp = BindResp(pdu['header']['command_id']+'_resp', "ESME_ROK", pdu['header']['sequence_number']).obj
        if pdu['header']['sequence_number'] != pdu_resp['header']['sequence_number']:
            raise Exception("invalid resp sequence_number")
        
        d = self.smmpRespPduSend(pdu_resp)
        timer = reactor.callLater(10, d.errback, Exception("timeout"))

        def onError(reason):
            if timer.active(): timer.cancel()
            self.loseConnection(reason)
            #return reason
        def onSent(arg):
            if timer.active(): timer.cancel()
            
            if pdu_resp['header']["command_status"] != "ESME_ROK":
                raise Exception("bind got status: " + pdu_resp['header']["command_status"])
            
            if pdu['header']['command_id'] == 'bind_receiver':
                self._newState('BIND_RX')
            if pdu['header']['command_id'] == 'bind_transmitter':
                self._newState('BIND_TX')
            if pdu['header']['command_id'] == 'bind_transceiver':
                self._newState('BIND_TRX')
            
            return arg

        d.addCallback(onSent)
        d.addErrback(onError)
        return d
    
    def onOtherPdu(self, pdu):
        pass

    #helpers    
    def _errbackAllWaiting(self, arg):
        for i in self.waiting_valuable.keys():
            (pdu, d) = self.waiting_valuable[i]
            del self.waiting_valuable[i]
            log.msg("errback for val_pdu %s" % (repr(pdu),))
            d.errback(arg)
        for i in self.waiting_resp.keys():
            (pdu, d) = self.waiting_resp[i]
            del self.waiting_resp[i]
            log.msg("errback for resp_pdu %s" % (repr(pdu),))
            d.errback(arg)
    
    @staticmethod
    def _nextSequenceNumber(sequence_number):
        return sequence_number + 1

    @staticmethod
    def _isRespPdu(pdu):
        return pdu['header']['command_id'].endswith('_resp')

    def _newState(self, state):
        log.msg("new state %s" % (state,))
        self.state=state

    def _canBeIssuedByEsme(self, pdu):
        command_id = pdu['header']['command_id'] 
        if self.state in ['OPEN']:
            return command_id in [
                'bind_transmitter',
                'bind_receiver',
                'bind_transceiver']        
        if self.state in ['BOUND_RX']:
            return command_id in [
                'unbind',
                'unbind_resp',
                'data_sm',
                'data_sm_resp',
                'deliver_sm_resp',
                'enquire_link',
                'enquire_link_resp',
                'generic_nack',]        
        if self.state in ['BOUND_TX']:
            return command_id in [
                'unbind',
                'unbind_resp',
                'submit_sm',
                'submit_sm_multi',
                'data_sm',
                'data_sm_resp',
                'query_sm',
                'cancel_sm',
                'replace_sm',
                'enquire_link',
                'enquire_link_resp',
                'generic_nack',]        
        if self.state in ['BOUND_TRX']:
            return command_id in [
                'unbind',
                'unbind_resp',
                'submit_sm',
                'submit_sm_multi',
                'data_sm',
                'data_sm_resp',
                'deliver_sm_resp',
                'query_sm',
                'cancel_sm',
                'enquire_link',
                'enquire_link_resp',
                'generic_nack',]

    def _canBeIssuedBySMSC(self, pdu):
        command_id = pdu['header']['command_id']
        if self.state in ['OPEN']:
            return command_id in [
                'bind_transmitter_resp',
                'bind_receiver_resp',
                'bind_transceiver_resp',
                'outbind',]        
        if self.state in ['BOUND_RX']:
            return command_id in [
                'unbind',
                'unbind_resp',
                'data_sm',
                'data_sm_resp',
                'deliver_sm_resp',
                'enquire_link',
                'enquire_link_resp',
                'alert_notification',
                'generic_nack',]        
        if self.state in ['BOUND_TX']:
            return command_id in [
                'unbind',
                'unbind_resp',
                'submit_sm_resp',
                'submit_sm_multi_resp',
                'data_sm',
                'data_sm_resp',
                'query_sm_resp',
                'cancel_sm_resp',
                'replace_sm',
                'enquire_link',
                'enquire_link_resp',
                'generic_nack',]        
        if self.state in ['BOUND_TRX']:
            return command_id in [
                'unbind',
                'unbind_resp',
                'submit_sm_resp',
                'submit_sm_multi_resp',
                'data_sm',
                'data_sm_resp',
                'deliver_sm_resp',
                'query_sm_resp',
                'cancel_sm_resp',
                'enquire_link',
                'enquire_link_resp',
                'alert_notification',
                'generic_nack',]

    def _canBeIssuedByHost(self, pdu):
        return True

    def _canBeIssuedByPeer(self, pdu):
        return True


def smppTuneEsme():
    def _canBeSentByHost(self, pdu):
        return self._canBeIssuedByEsme(pdu)
    def _canBeSentByPeer(self, pdu):
        return self._canBeIssuedBySmsc(pdu)

def smppTuneSmsc():
    def _canBeSentByHost(self, pdu):
        return self._canBeIssuedBySmsc(pdu)
    def _canBeSentByPeer(self, pdu):
        return self._canBeIssuedByEsme(pdu)
