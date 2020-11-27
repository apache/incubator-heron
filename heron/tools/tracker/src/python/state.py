"""
This module holds global state which is initialised on entry.

"""
from heron.tools.tracker.src.python.tracker import Tracker

# this is populated on entry into the application
tracker: Tracker = None
