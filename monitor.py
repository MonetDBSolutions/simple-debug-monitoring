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

dbpathCommand = "ps auxwww | grep mserver5 | grep dbpath"
result = subprocess.run(dbpathCommand, shell=True, check=True, executable=SHELL, stdout=subprocess.PIPE, stderr=DEVNULL, encoding='ascii')
if result.returncode == 0:
    for s in result.stdout.split():
        if "--dbpath=" in s:
            DBPATH=s[9:]
            break;
    if DBPATH == None or not DBPATH.strip():
        print("DBPATH not found in process info \"{res}\"".fomat(res=result.stdout))
        exit(1)
else:
    print("Failed to execute \"{cmd}\"".fomat(cmd=dbpathCommand))
    exit(1)


# bash command that gives: date in sec in ISO 8601 format; disksize in bytes; RSS in #pages; VM in #pages; #mmapped files
statsCommand = """
echo  $(date -Iseconds) $(du -bs {db_path} | cut -f1) $(cat /proc/$(pgrep mserver5)/statm | cut -d ' ' -f2) $(cat /proc/$(pgrep mserver5)/statm | cut -d ' ' -f1) $(cat /proc/$(pgrep mserver5)/maps | wc -l);
""".format(db_path=DBPATH)

with open(sys.argv[1], 'a') as log:
    while True:
        time.sleep(5);
        result = subprocess.run(statsCommand, shell=True, check=True, executable=SHELL, stdout=subprocess.PIPE, stderr=DEVNULL, encoding='ascii')
        if result.returncode == 0:
            stats = result.stdout

        print(stats)
        log.write(stats)

