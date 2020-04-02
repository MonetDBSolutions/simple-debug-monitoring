#!/usr/bin/env python3

import PyQt5
import matplotlib
import collections
import time
import os
import sys

import matplotlib.pyplot as plt
import matplotlib.animation as animation
from datetime import datetime

matplotlib.use('Qt5Agg')

#amount of data to be displayed at once, this is the size of the x axis
#increasing this amount also makes plotting slightly slower
data_amount = 5000
PAGE_SIZE=4096

"""
#set the size of the deque objects
datalist = collections.deque([],data_amount)
disksize = collections.deque([],data_amount)
RSSsize = collections.deque([],data_amount)
VMsize = collections.deque([],data_amount)
"""

#set the size of the list objects
datalist = list()
disksize = list()
RSSsize = list()
VMsize = list()

#configure the graph itself
fig, ax = plt.subplots()
ax.grid()
ax.xaxis_date()
#ax.xaxis.set_major_formatter(myFmt)

# define a plot for each metric
size_total_db_directory_scatter_plot, = ax.plot([], [], 'b-o', ls="", label='Size of db directory (in bytes)')
RSS_plot, = ax.plot([], [], 'r-o', ls="", label='RSS monetdb (in pages of {page_size} bytes)'.format(page_size=PAGE_SIZE))
VM_plot, = ax.plot([], [], 'g-o', ls="", label='VM monetdb (in pages of {page_size} bytes)'.format(page_size=PAGE_SIZE))

# bash command that gives 
bashCommand = """
echo $(du -s {db_path} | cut -f1) $(cat /proc/{pid}/statm | cut -d ' ' -f2) $(cat /proc/{pid}/statm | cut -d ' ' -f1);
""".format(db_path=sys.argv[1], pid=sys.argv[2])

devnull = open(os.devnull, 'w')

def get_metrics():
    import subprocess
    shell = '/bin/bash'
    result = subprocess.run(bashCommand, shell=True, check=True, executable=shell, stdout=subprocess.PIPE, stderr=devnull, encoding='ascii')

    return result.stdout if result.returncode == 0 else None


import random
def update(data):
    datalist.append(data[0])
    disksize.append(data[1])
    RSSsize.append(data[2])
    VMsize.append(data[3])
    size_total_db_directory_scatter_plot.set_data(datalist, disksize)
    RSS_plot.set_data(datalist, RSSsize)
    VM_plot.set_data(datalist, VMsize)
    ax.relim()
    ax.autoscale_view(True,True,True)

def data_gen():
    while True:
        """
        We read two data points in at once, to improve speed
        You can read more at once to increase speed
        Or you can read just one at a time for improved animation smoothness
        data from the pipe comes in as a string,
        and is seperated with a newline character,
        which is why we use respectively eval and rstrip.
        """
        
        raw_record=get_metrics()
        if raw_record:
            time = datetime.now()
            print(time, raw_record)
            fields = raw_record.split()
            yield (time, int(fields[0]), int(fields[1]), int(fields[2]))
ani = animation.FuncAnimation(fig,update, interval=500, frames=data_gen, repeat=True, blit=False)
plt.legend()
plt.show()
