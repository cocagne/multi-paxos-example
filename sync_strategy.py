
# This module provides a simple implementation of a catch-up mechanism
# that synchronizes the peer with the current state of the multi-paxos
# chain when it detects that the peer has fallen behind.
#
import random

from twisted.internet import task

class SimpleSynchronizationStrategyMixin (object):
    
    sync_delay = 10.0
    
    def set_messenger(self, messenger):
        super(SimpleSynchronizationStrategyMixin,self).set_messenger(messenger)

        def sync():
            self.messenger.send_sync_request(random.choice(self.peers), self.instance_number)
                
        self.sync_task = task.LoopingCall(sync)
        self.sync_task.start(self.sync_delay)

        
    def receive_sync_request(self, from_uid, instance_number):
        if instance_number < self.instance_number:
            self.messenger.send_catchup(from_uid, self.instance_number, self.current_value)


    def receive_catchup(self, from_uid, instance_number, current_value):
        if instance_number > self.instance_number:
            print 'SYNCHRONIZED: ', instance_number, current_value
            self.advance_instance(instance_number, current_value, catchup=True)
