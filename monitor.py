#!/usr/bin/env python3

import time
import os
import sys
from datetime import datetime
import subprocess

PAGE_SIZE=4096
SHELL = '/bin/bash'
DEVNULL = open(os.devnull, 'w')
DBPATH = None

if len(sys.argv) != 2:
    print("usage: ./monitor.py <path/to/logfilename>")
    exit(0)

dbpathCommand = "ps auxwww | grep mserver5 | grep dbpath"
result = subprocess.run(dbpathCommand, shell=True, check=True, executable=SHELL, stdout=subprocess.PIPE, stderr=DEVNULL, encoding='ascii')
if result.returncode == 0:
    for s in result.stdout.split():
        if "--dbpath=" in s:
            DBPATH=s[9:]
            break;
    if DBPATH == None or not DBPATH.strip():
        print("[{now}] ERROR!DBPATH not found in process info \"{res}\"".fomat(now=datetime.now(), res=result.stdout))
        exit(1)
else:
    print("[{now}] ERROR!Failed to execute: {cmd}".format(now=datetime.now(), cmd=dbpathCommand))
    exit(1)


# bash command that gives: date in sec in ISO 8601 format; disksize in bytes; RSS in #pages; VM in #pages; #mmapped files; #fd
statsCommand = """
echo  $(date -Iseconds) $(du -bs {db_path} | cut -f1) $(cat /proc/$(pgrep mserver5)/statm | cut -d ' ' -f2) $(cat /proc/$(pgrep mserver5)/statm | cut -d ' ' -f1) $(cat /proc/$(pgrep mserver5)/maps | wc -l) $(ls -al  /proc/$(pgrep mserver5)/fd | wc -l) ;
""".format(db_path=DBPATH)

def do_log(cmd, outfile):
    res = subprocess.run(cmd, shell=True, check=True, executable=SHELL, stdout=subprocess.PIPE, stderr=DEVNULL, encoding='ascii')
    if res.returncode == 0:
        print(res.stdout)
        log.write(res.stdout)
        return res.stdout
    else:
        log.write("[{now}] ERROR!Failed to execute: {cmd}".format(now=datetime.now(), cmd=cmd))
        return None

with open(sys.argv[1], 'a') as log:
    nextmaps = 5000
    nextfds = 100
    while True:
        time.sleep(5)
        res = do_log(statsCommand, log)
        if res == None:
            continue;
        stats = res.split()
        maps = int(stats[4])
        fd = int(stats[5])

        # $ sysctl vm.max_map_count ==> vm.max_map_count = 65530
        # log the mmaps every 5000 or just before hitting the limit
        if maps > nextmaps or maps > 65525:
            cmd = "cat /proc/$(pgrep mserver5)/maps"
            log.write("\n{cmd}\n".format(cmd=cmd))
            do_log(cmd, log)
            nextmaps += 5000

        # $ ulimit -n ==> 1024
        # log the fds every 100 or just before hitting the limit
        if fd > nextfds or fd > 1022:
            cmd = "ls -al /proc/$(pgrep mserver5)/fd"
            log.write("\n{cmd}\n".format(cmd=cmd))
            do_log(cmd, log)
            nextfds += 100
