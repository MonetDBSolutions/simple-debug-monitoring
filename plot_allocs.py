#!/usr/bin/env python3

import PyQt5
import matplotlib
import collections
import time
import os
import sys
import glob
import math
from datetime import datetime

import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.dates as mdates
matplotlib.use('Qt5Agg')

from allocation_index import create_graphs

trace_file = sys.argv[1]

data = create_graphs(trace_file)

def calculate_major_formatter(max_ts, min_ts):
    duration = max_ts - min_ts
    if duration.days >= 1:
        return '%H'
    if duration.seconds / 3600 >= 1:
        return '%M'
    else:
        return '%S'

date_format = calculate_major_formatter(data.max_ts, data.min_ts)
max_nr_plots = 100
box_size = math.ceil(min(len(data.items()), max_nr_plots)**(1/2.0))
time_formatter = mdates.DateFormatter(date_format)
subplot_index = 1
nr_plots = 0

for alloc, graph in sorted(data.items()):
    x = graph.t
    y = graph.c

    if graph.c[-1] == graph.c[0]:
        continue
    axes = plt.subplot(box_size, box_size, subplot_index, label="allocs in bytes.")
    axes.xaxis.set_major_formatter(time_formatter)
    axes.set_xlim([data.min_ts, data.max_ts])
    subplot_index = subplot_index + 1
    plt.plot(x, y, 'o-')
    plt.title(alloc)
    nr_plots = nr_plots + 1
    if nr_plots == max_nr_plots:
        break

plt.show()
