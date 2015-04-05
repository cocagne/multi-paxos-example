import sys
import os.path
import argparse
import json

from twisted.internet import reactor

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.dirname(this_dir) )

import config

from replicated_value    import BaseReplicatedValue
from messenger           import Messenger
from sync_strategy       import SimpleSynchronizationStrategyMixin
from resolution_strategy import ExponentialBackoffResolutionStrategyMixin
from master_strategy     import DedicatedMasterStrategyMixin


p = argparse.ArgumentParser(description='Multi-Paxos replicated value server')
p.add_argument('uid', choices=['A', 'B', 'C'], help='UID of the server. Must be A, B, or C')
p.add_argument('--master', action='store_true', help='If specified, a dedicated master will be used. If one server specifies this flag, all must')

args = p.parse_args()


if args.master:

    class ReplicatedValue(DedicatedMasterStrategyMixin, ExponentialBackoffResolutionStrategyMixin, SimpleSynchronizationStrategyMixin, BaseReplicatedValue):
        '''
        Mixes the dedicated master, resolution, and synchronization strategies into the base class
        '''
else:
    
    class ReplicatedValue(ExponentialBackoffResolutionStrategyMixin, SimpleSynchronizationStrategyMixin, BaseReplicatedValue):
        '''
        Mixes just the resolution and synchronization strategies into the base class
        '''


state_file = config.state_files[args.uid]


r = ReplicatedValue(args.uid, config.peers.keys(), state_file)
m = Messenger(args.uid, config.peers, r)

reactor.run()

