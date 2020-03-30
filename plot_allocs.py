#!/usr/bin/env python3

import PyQt5
import matplotlib
import collections
import time
import os
import sys
import glob

import matplotlib.pyplot as plt
import matplotlib.animation as animation
from datetime import datetime

matplotlib.use('Qt5Agg')

data = {}

indices = list()

ALLOC_DATA_FILE_TEMPLATE="/home/aris/sources/simple-debug-monitoring/allocs_rest/allocs_{index}.txt"
for fname in sorted(glob.glob(ALLOC_DATA_FILE_TEMPLATE.format(index='*'))):


    index = int(fname.split('/')[-1].split('.')[0].split('_')[1])
    indices.append(index)

for index in sorted(indices):
    fname = ALLOC_DATA_FILE_TEMPLATE.format(index=index)
    with open(fname, 'r') as file:
        for line in file:

            alloc_count = line.rstrip().split()

            alloc = data.setdefault(int(alloc_count[0]), {'index': list(), 'count': list()})

            alloc['index'].append(index)
            alloc['count'].append(int(alloc_count[1]))


import math
box_size = math.ceil((len(data.items())**(1/2.0)))
subplot_index = 1
for alloc, graph in data.items():
    x = graph['index']
    y = graph['count']
    plt.subplot(box_size, box_size, subplot_index, label="allocs in bytes.")
    subplot_index = subplot_index + 1
    plt.plot(x, y, 'o-')
    plt.title(alloc)

plt.show()