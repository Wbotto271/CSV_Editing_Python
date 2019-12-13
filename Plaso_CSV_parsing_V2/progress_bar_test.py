from __future__ import print_function

import functools
import random
import sys
import time

import progressbar

def animated_marker():
    bar = progressbar.ProgressBar(
    widgets=['Working: ', progressbar.AnimatedMarker()])
    for i in bar((i for i in range(5))):
        time.sleep(0.1)
animated_marker()
