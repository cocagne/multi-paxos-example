# This module provides a base class for maintaining a single replicated
# value via multi-paxos. The responsibilities of this class are:
#
#    * Loading and saving the state to/from disk
#    * Maintaining the integrity of the multi-paxos chain
#    * Bridging the composable_paxos.PaxosInstance object for the current link
#      in the multi-paoxs chain with the Messenger object used to send and
#      receive messages over the network.
#
# In order to provide clean separation-of-concerns, this class is completely
# passive. Active operations like the logic used to ensure that resolution
# is achieved and catching up after falling behind are left to Mixin classes.

import os
import json
import random
import os.path

from twisted.internet import reactor, defer, task

from composable_paxos import PaxosInstance, ProposalID, Prepare, Nack, Promise, Accept, Accepted, Resolution


class BaseReplicatedValue (object):

    def __init__(self, network_uid, peers, state_file):
        self.messenger   = None
        self.network_uid = network_uid
        self.peers       = peers            # list of peer network uids
        self.quorum_size = len(peers)/2 + 1
        self.state_file  = state_file

        self.load_state()

        self.paxos = PaxosInstance(self.network_uid, self.quorum_size,
                                   self.promised_id, self.accepted_id,
                                   self.accepted_value)


    def set_messenger(self, messenger):
        self.messenger = messenger

        
    def save_state(self, instance_number, current_value, promised_id, accepted_id, accepted_value):
        '''
        For crash recovery purposes, Paxos requires that some state be saved to
        persistent media prior to sending Promise and Accepted messages. We'll
        also save the state of the multi-paxos chain here so everything is kept
        in one place.
        '''
        self.instance_number = instance_number
        self.current_value   = current_value
        self.promised_id     = promised_id
        self.accepted_id     = accepted_id
        self.accepted_value  = accepted_value

        tmp = self.state_file + '.tmp'
        
        with open(tmp, 'w') as f:
            f.write( json.dumps( dict(instance_number = instance_number,
                                      promised_id     = promised_id,
                                      accepted_id     = accepted_id,
                                      accepted_value  = accepted_value,
                                      current_value   = current_value) ) )
            f.flush()
            os.fsync(f.fileno()) # Wait for the data to be written to disk
            
        # os.rename() is an atomic filesystem operation. By writing the new
        # state to a temporary file and using this method, we avoid the potential
        # for leaving a corrupted state file in the event that a crash/power loss
        # occurs in the middle of update.
        os.rename(tmp, self.state_file)

        
    def load_state(self):
        if not os.path.exists(self.state_file):
            with open(self.state_file, 'w') as f:
                f.write( json.dumps( dict(instance_number = 0,
                                          promised_id     = None,
                                          accepted_id     = None,
                                          accepted_value  = None,
                                          current_value   = None) ) )
                f.flush()

        with open(self.state_file) as f:
            m = json.loads(f.read())

            def to_pid(v):
                return ProposalID(*v) if v else None

            self.instance_number = m['instance_number']
            self.current_value   = m['current_value']
            self.promised_id     = to_pid(m['promised_id'])
            self.accepted_id     = to_pid(m['accepted_id'])
            self.accepted_value  = m['accepted_value']


    def propose_update(self, new_value):
        """
        This is a key method that some of the mixin classes override in order
        to provide additional functionality when new values are proposed
        """
        if self.paxos.proposed_value is None:
            self.paxos.propose_value( new_value )


    def advance_instance(self, new_instance_number, new_current_value, catchup=False):
        self.save_state(new_instance_number, new_current_value, None, None, None)
        
        self.paxos = PaxosInstance(self.network_uid, self.quorum_size, None, None, None)

        print 'UPDATED: ', new_instance_number, new_current_value


    def send_prepare(self, proposal_id):
        for uid in self.peers:
            self.messenger.send_prepare(uid, self.instance_number, proposal_id)


    def send_accept(self, proposal_id, proposal_value):
        for uid in self.peers:
            self.messenger.send_accept(uid, self.instance_number, proposal_id, proposal_value)

            
    def send_accepted(self, proposal_id, proposal_value):
        for uid in self.peers:
            self.messenger.send_accepted(uid, self.instance_number, proposal_id, proposal_value)


    def receive_prepare(self, from_uid, instance_number, proposal_id):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return
        
        m = self.paxos.receive_prepare( Prepare(from_uid, proposal_id) )
        
        if isinstance(m, Promise):
            self.save_state(self.instance_number, self.current_value, m.proposal_id,
                            m.last_accepted_id, m.last_accepted_value)
            
            self.messenger.send_promise(from_uid, self.instance_number,
                                        m.proposal_id, m.last_accepted_id, m.last_accepted_value )
        else:
            self.messenger.send_nack(from_uid, self.instance_number, proposal_id, self.promised_id)


    def receive_nack(self, from_uid, instance_number, proposal_id, promised_proposal_id):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return

        self.paxos.receive_nack( Nack(from_uid, self.network_uid, proposal_id, promised_proposal_id) )
        
            
    def receive_promise(self, from_uid, instance_number, proposal_id, last_accepted_id, last_accepted_value):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return

        m = self.paxos.receive_promise( Promise(from_uid, self.network_uid, proposal_id,
                                                last_accepted_id, last_accepted_value) )
        
        if isinstance(m, Accept):
            self.send_accept(m.proposal_id, m.proposal_value)

            
    def receive_accept(self, from_uid, instance_number, proposal_id, proposal_value):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return

        m = self.paxos.receive_accept( Accept(from_uid, proposal_id, proposal_value) )
        
        if isinstance(m, Accepted):
            self.save_state(self.instance_number, self.current_value, self.promised_id,
                            proposal_id, proposal_value)
            self.send_accepted(m.proposal_id, m.proposal_value)
        else:
            self.messenger.send_nack(from_uid, self.instance_number, proposal_id, self.promised_id)

            
    def receive_accepted(self, from_uid, instance_number, proposal_id, proposal_value):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return

        m = self.paxos.receive_accepted( Accepted(from_uid, proposal_id, proposal_value) )
        
        if isinstance(m, Resolution):
            self.advance_instance( self.instance_number + 1, proposal_value )
    
