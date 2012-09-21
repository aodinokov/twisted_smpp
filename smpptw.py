
from smpp.pdu_builder import *

from twisted.internet import reactor
from twisted.internet.protocol import Protocol
from twisted.internet.defer import Deferred, succeed, fail
from twisted.python.failure import Failure
from twisted.python import log

#TO Fix: 
#1. only version 34
#2. internal packet filter can fail(incorrect current state)
#3. problems with error handling during session (we need one function with Failure instead of many)
#4. client and server factories are too complicated
#5. probably we have some problems with sequence_number


class SmppGeneric(Protocol):

    def connectionMade(self):
        self.defer={}
        self.waiting_resp={}
        
        self.data = ''        
        self.state='OPEN'
        
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
            self.smmpCantParseInputData(cur_data)
        else: 
            log.msg("got SMPP PDU")
            self.smppPduReceived(pdu)
 
    def connectionLost(self, reason):
        self.state='CLOSED'
    
    def smmpCantParseInput(self, data):
        pass

    def smmpIcorrectPduCommandId(self, pdu):
        pass

    def _canBeIssuedByHost(self, pdu):
        return True

    def _canBeIssuedByPeer(self, pdu):
        return True

# low level interface    
    def smmpPduSend(self, pdu):
        log.msg("sending SMPP PDU")
        if not (self._canBeIssuedByHost(pdu)):
            raise Exception("PDU can't be sent by host")
        self.transport.write(pack_pdu(pdu))
        log.msg("sent SMPP PDU")    

    def smppPduReceived(self, pdu):
        if not (self._canBeIssuedByPeer(pdu)):
            return self.smmpIcorrectPduCommandId(pdu)
            
        sequence_number = int(pdu['header']['sequence_number'])
        if not self._isRespPdu(pdu):
            if not (sequence_number == self.sequence_number_rx + 1):
                log.msg("incorrect sequence_number")
            else:
                self.sequence_number_rx = sequence_number 
                self.smppValuablePduReceived(pdu)
        else:
            if not (sequence_number == self.sequence_number_tx_resp + 1):
                log.msg("incorrect sequence_number")
            else:
                self.sequence_number_tx_resp = sequence_number
                if sequence_number in self.defer:
                    d = self.defer[sequence_number]
                    del self.defer[sequence_number]
                    d.callback(pdu)
                self.smppRespPduReceived(pdu)

# mid level interface
    def smmpValuablePduSend(self, pdu):
        log.msg("smmpValuablePduSend")
        if self._isRespPdu(pdu):
            raise Exception("PDU can't be resp")
        self.sequence_number_tx += 1
        pdu['header']['sequence_number'] = self.sequence_number_tx # str(self.sequence_number_tx)
        try:
            self.smmpPduSend(pdu)
        except Exception as e:
            import sys
            import traceback
            x = sys.exc_info()
            log.msg("x %s" % (repr(traceback.format_tb(x[2])),))
            # TODO: decrease sequence_number_tx?
            raise e

    def smmpValuablePduSendAndWaitResp(self, pdu):
        log.msg("smmpValuablePduSendAndWaitResp")
        if self._isRespPdu(pdu):
            raise Exception("PDU can't be resp")
        d = Deferred()
        try:
            self.smmpValuablePduSend(pdu)
        except Exception as e:
            return fail(e)
        
        #reactor.callLater(10, d.errback, Exception("response timeout"))
        
        self.defer[self.sequence_number_tx] = d
        return d
        
    def smmpRespPduSend(self, pdu):
        log.msg("smmpRespPduSend")
        if not self._isRespPdu(pdu):
            raise Exception("PDU has to be resp")
        if int(pdu['header']['sequence_number']) == self.sequence_number_rx_resp + 1:
            self.sequence_number_rx_resp += 1
            try:
                self.smmpPduSend(pdu)
                res = succeed(pdu)
            except Exception as e:
                res = fail(e)
                import sys
                import traceback
                x = sys.exc_info()
                log.msg("%s %s\n%s" % (x[0], x[1], repr(traceback.format_tb(x[2])),))
            while self.sequence_number_rx_resp in self.waiting_resp:
                log.msg("sending waiting SMPP PDU resp")
                (lpdu, d) = self.waiting_resp[self.sequence_number_rx_resp]
                del self.waiting_resp[self.sequence_number_rx_resp]
                self.sequence_number_rx_resp += 1
                try:
                    self.smmpPduSend(lpdu)
                    d.callback(lpdu)
                except Exception as e:
                    d.errback(e)
            return res
        log.msg("queueing SMPP PDU resp")
        d = Deferred()
        self.waiting_resp[pdu['header']['sequence_number']] = (pdu,d)
        return d 

    def smppValuablePduReceived(self, pdu):
        if pdu['header']['command_id'] in ['unbind']:
            self.smppUnbindReceived(pdu)        

    def smppRespPduReceived(self, pdu):
        pass

    # high level interface
    def loseConnection(self, arg=None):
        """ forced connection loss (without unbind)
        """
        log.msg("loseConnection arg %s" % (repr(arg)))
        for d in self.defer.keys():
            d.errback(arg)        
        self.transport.loseConnection(arg)
        return arg

    def unbind(self):
        """ soft protocol shutdown
        """
        pdu = Unbind(self.sequence_number_tx).obj
        d = self.smmpValuablePduSendAndWaitResp(pdu)
        
        timer = reactor.callLater(10, d.errback, Exception("timeout"))

        def onError(err):
            if timer.active(): timer.cancel()
            return err
        def onSuccess(val):
            if timer.active(): timer.cancel()
            return None

        d.addCallbacks(onSuccess, onError)
        d.addBoth(self.loseConnection)
        return d

    def smppUnbindReceived(self, pdu):
        pdu_resp = PDU('unbind_resp', "ESME_ROK", pdu['header']['sequence_number']).obj
        d = self.smmpRespPduSend(pdu_resp)
        d.addBoth(self.transport.loseConnection)

    #helpers
    def _isRespPdu(self, pdu):
        return pdu['header']['command_id'].endswith('_resp')

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
