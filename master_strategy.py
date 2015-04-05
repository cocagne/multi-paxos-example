# This module provides an optional Mixin class that implements master leases
# and single-round-trip resolution on Paxos instances while the lease is held.
#
import json
import random
import os.path
import time

from twisted.internet import reactor, defer, task

from composable_paxos import ProposalID, Prepare, Promise


        
class DedicatedMasterStrategyMixin (object):

    lease_window   = 10.0  # seconds
    lease_start    = 0.0
    lease_expiry   = None
    master_uid     = None  # While None, no peer holds the master lease

    master_attempt = False # Limits peer attempts to become the master

    _initial_load  = True


    def start_master_lease_timer(self):
        self.lease_start = time.time()
        
        if self.lease_expiry is not None and self.lease_expiry.active():
            self.lease_expiry.cancel()
            
        self.lease_expiry = reactor.callLater(self.lease_window, self.lease_expired)

        
    def update_lease(self, master_uid):
        self.master_uid = master_uid

        if self.network_uid != master_uid:
            self.start_master_lease_timer()

        if master_uid == self.network_uid:
            renew_delay = (self.lease_start + self.lease_window - 1) - time.time()
            
            if renew_delay > 0:
                reactor.callLater(renew_delay, lambda : self.propose_update(self.network_uid, False))
            else:
                self.propose_update(self.network_uid, False)


    def lease_expired(self):
        self.master_uid = None
        self.propose_update( self.network_uid, False )
    

    #--------------------------------------------------------------------------------
    # Method Overrides
    #
    def propose_update(self, new_value, application_level=True):
        """
        All values are tuples. If the left value is set, it's an attempt to gain master status.
        If the right value is set, it's an application-level value
        """
        if application_level:
            if self.master_uid == self.network_uid:
                super(DedicatedMasterStrategyMixin,self).propose_update( json.dumps( [None,new_value] ) )
            else:
                print 'IGNORING CLIENT REQUEST. Current master is: ', self.master_uid
        else:
            if (self.master_uid is None or self.master_uid == self.network_uid) and not self.master_attempt:
                self.master_attempt = True
                self.start_master_lease_timer()
                super(DedicatedMasterStrategyMixin,self).propose_update( json.dumps( [new_value,None] ) )


    def load_state(self):
        super(DedicatedMasterStrategyMixin,self).load_state()

        if self._initial_load:
            self._initial_load = False
            self.update_lease(None)


    def drive_to_resolution(self):
        """
        Note: this overrides the method defined in ResolutionStrategyMixin
        """
        if self.master_uid == self.network_uid:
            self.stop_driving()

            if self.paxos.proposal_id.number == 1:
                self.send_accept(self.paxos.proposal_id, self.paxos.proposed_value)
            else:
                self.paxos.prepare()

            self.retransmit_task = task.LoopingCall( lambda : self.send_prepare(self.paxos.proposal_id) )
            self.retransmit_task.start( self.retransmit_interval/1000.0, now=False )
        else:
            super(DedicatedMasterStrategyMixin,self).drive_to_resolution()
        

    def advance_instance(self, new_instance_number, new_current_value, catchup=False):

        self.master_attempt = False

        if catchup:
            super(DedicatedMasterStrategyMixin,self).advance_instance(new_instance_number, new_current_value)
            self.paxos.prepare() # ensure we won't send any prepare messages with ID 1 (might conflict with the current master)
            return
        
        t = json.loads(new_current_value) # Returns a list: [master_uid, application_value]. Only one element will be valid

        if t[0] is not None:
            print '   Lease Granted: ', t[0]
            self.update_lease( t[0] )
            
            new_current_value = self.current_value
        else:
            print '   Application Value:', t[1]
            new_current_value = t[1]

        super(DedicatedMasterStrategyMixin,self).advance_instance(new_instance_number, new_current_value)

        if self.master_uid:

            master_pid = ProposalID(1,self.master_uid)
            
            if self.master_uid == self.network_uid:
                self.paxos.prepare()
                
                for uid in self.peers:
                    self.paxos.receive_promise( Promise(uid, self.network_uid, master_pid, None, None) )
            else:
                self.paxos.receive_prepare( Prepare(self.master_uid, master_pid) )
                self.paxos.observe_proposal( master_pid )


    def receive_prepare(self, from_uid, instance_number, proposal_id):
        
        if self.master_uid and from_uid != self.master_uid:
            return # Drop non-master requests

        super(DedicatedMasterStrategyMixin,self).receive_prepare(from_uid, instance_number, proposal_id)

        
    def receive_accept(self, from_uid, instance_number, proposal_id, proposal_value):

        if self.master_uid and from_uid != self.master_uid:
            return # Drop non-master requests

        super(DedicatedMasterStrategyMixin,self).receive_accept(from_uid, instance_number, proposal_id, proposal_value)
