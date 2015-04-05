# This module provides a Mixin class that implements a simple EponentialBackoff
# mechanism for preventing continual collisions between peers attempting to
# drive a Paxos instance to resolution. Also, once the peer becomes aware of a
# proposal, either by suggesting one itself or by observing one on the network,
# it will continually monitor the messaging activity for that instance. If the
# messages drop off for a while, this implementation will assume that the
# current driver has failed and will attempt to step in to take over the
# resolution process.
#
import random

from twisted.internet import reactor, defer, task


class ExponentialBackoffResolutionStrategyMixin (object):

    # All times are in milliseconds
    backoff_initial       =    5 
    backoff_cap           = 2000 
    drive_silence_timeout = 3000 
    retransmit_interval   = 1000
    
    backoff_window        = backoff_initial
    retransmit_task       = None
    delayed_drive         = None


    def reschedule_next_drive_attempt(self, delay):
        if self.delayed_drive is not None and self.delayed_drive.active():
            self.delayed_drive.cancel()
            
        self.delayed_drive = reactor.callLater(delay, self.drive_to_resolution)

        
    def drive_to_resolution(self):
        self.stop_driving()
        
        m = self.paxos.prepare() # Advances to the next proposal number

        self.retransmit_task = task.LoopingCall( lambda : self.send_prepare(m.proposal_id) )
        self.retransmit_task.start( self.retransmit_interval/1000.0, now=True )

        
    def stop_driving(self):
        
        if self.retransmit_task is not None:
            self.retransmit_task.stop()
            self.retransmit_task = None

        if self.delayed_drive is not None and self.delayed_drive.active():
            self.delayed_drive.cancel()


    #--------------------------------------------------------------------------------
    # Method Overrides
    #
    def advance_instance(self, new_instance_number, new_current_value, catchup=False):
        super(ExponentialBackoffResolutionStrategyMixin,self).advance_instance(new_instance_number, new_current_value, catchup=catchup)

        self.stop_driving()
        self.backoff_window = self.backoff_initial

        
    def propose_update(self, new_value):
        super(ExponentialBackoffResolutionStrategyMixin,self).propose_update(new_value)
        self.drive_to_resolution()


    def send_accept(self, proposal_id, proposal_value):
        if self.retransmit_task is not None:
            self.retransmit_task.stop()

        self.retransmit_task = task.LoopingCall( lambda : super(ExponentialBackoffResolutionStrategyMixin,self).send_accept(proposal_id, proposal_value) )
        self.retransmit_task.start( self.retransmit_interval, now=True )

        
    def receive_accept(self, from_uid, instance_number, proposal_id, proposal_value):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return
        
        super(ExponentialBackoffResolutionStrategyMixin,self).receive_accept(from_uid, instance_number, proposal_id, proposal_value)

        # The peer proposing the value could fail before resolution is achieved. Step in to complete the process if
        # the drive_silence_timeout elapses with no messages received
        self.reschedule_next_drive_attempt( self.drive_silence_timeout/1000.0 )
        
        
    def receive_nack(self, from_uid, instance_number, proposal_id, promised_proposal_id):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return

        super(ExponentialBackoffResolutionStrategyMixin,self).receive_nack(from_uid, instance_number, proposal_id, promised_proposal_id)

        self.stop_driving()
        
        self.backoff_window = self.backoff_window * 2
        
        if self.backoff_window > self.backoff_cap:
            self.backoff_window = self.backoff_cap

        self.reschedule_next_drive_attempt( (self.backoff_window * random.random())/1000.0 )
