import os, signal, sys
import time, random

TIME_RANGES_3 = {
    "tenMinMetric" :       (0,        10*60,    'm'),
    "threeHourMetric" :    (0,        3*60*60,  'h'),
    "oneDayMetric" :       (0,        24*60*60, 'h'),
}

TIME_RANGES_6 = {
    "tenMinMetric" :       (0,        10*60,    'm'),
    "threeHourMetric" :    (0,        3*60*60,  'h'),
    "oneDayMetric" :       (0,        24*60*60, 'h'),

    "prevTenMinMetric":    (10*60,    20*60,    'm'),
    "prevThreeHourMetric": (3*60*60,  6*60*60,  'h'),  
    "prevOneDayMetric" :   (24*60*60, 48*60*60, 'h')  
}

def get_time_ranges(ranges):

    # get the current time
    now = int(time.time())

    # form the new 
    time_slots = dict()

    for key, value in ranges.iteritems():
        time_slots[key] = (now - value[0], now-value[1], value[2])

    return (now, time_slots)
