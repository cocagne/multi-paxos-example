# This module provides a very simple client interface for suggesting new
# replicated values to one of the servers. No reply is received so an eye must
# be kept on the server output to see if the new suggestion is received. Also,
# when master leases are in use, requests must be sent to the current master
# server. All non-master servers will ignore the requests since they do not have
# the ability to propose new values in the multi-paxos chain.

import sys

from twisted.internet import reactor, defer, protocol

import config

class ClientProtocol(protocol.DatagramProtocol):

    def __init__(self, uid, new_value):
        self.addr      = config.peers[uid]
        self.new_value = new_value

    def startProtocol(self):
        self.transport.write('propose {0}'.format(self.new_value), self.addr)
        reactor.stop()


if len(sys.argv) != 3 or not  sys.argv[1] in config.peers:
    print 'python client.py <A|B|C> <new_value>'
    sys.exit(1)

    
def main():
    reactor.listenUDP(0,ClientProtocol(sys.argv[1], sys.argv[2]))

    
reactor.callWhenRunning(main)
reactor.run()

