"""Integration test common spout"""
__all__ = ['identity_bolt', 'count_aggregator_bolt', 'word_count_bolt', 'double_tuples_bolt']

from .identity_bolt import IdentityBolt
from .count_aggregator_bolt import CountAggregatorBolt
from .word_count_bolt import WordCountBolt
from .double_tuples_bolt import DoubleTuplesBolt
