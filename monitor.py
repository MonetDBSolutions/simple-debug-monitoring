#!/usr/bin/env python3

import os, subprocess
import sys, getopt
import time
from datetime import datetime

assert sys.version_info >= (3, 6)


# global values
USAGE = """
Monitors resource usage of one running MonetDB database:
    Timestamp DBsize(GB) RSS(GB) VMsize(GB) #mmaps #FDs
(C) 2020 MonetDB Solutions B.V.

usage: ./monitor.py [options]

--help             Print this help screen
--dbname           Name of the database to monitor
--logbase          Extra base filename or path for the log file "<dbname>.log"
--log-interval     Log interval in seconds, default 5
--dbcheck-interval Interval in seconds to check database tables, default 1800
--mmap-increase    How often to log the memory mapped files, default per 5000 increases
--fd-increase      How often to log the file descripters, default per 100 increases
""".strip()
PAGE_SIZE=4096
GB_SIZE=1024*1024*1024
SHELL = '/bin/bash'
DEVNULL = open(os.devnull, 'w')
LOG_INTERVAL = 5 # sec
DBCHK_INTERVAL = 1800 # sec
MMAP_INCREASE = 5000
FD_INCREASE = 100

def mynow():
    return datetime.now().astimezone().replace(microsecond=0).isoformat()

def get_db_info(dbname):
    cmd = "ps auxwww | grep mserver5 | grep dbpath"
    if dbname:
        cmd = cmd + " | grep " + dbname

    res = subprocess.run(cmd, shell=True, check=True, executable=SHELL, stdout=subprocess.PIPE, stderr=DEVNULL, encoding='ascii')
    if res.returncode == 0:
        if res.stdout.count("--dbpath") > 1:
            exit_on_error("get_db_info(): more than 1 running database found in process info.: {info}".format(info=res.stdout))

        for s in res.stdout.split():
            if "--dbpath=" == s[0:9]:
                return s[9:]

        exit_on_error("!get_db_info(): could not find database")
    else:
        exit_on_error("!get_db_info(): failed to execute: {cmd}".format(cmd=cmd))
    return None, None #this should never be reached

def do_log(cmd, log, tee = True):
    ROUND = 2
    res = subprocess.run(cmd, shell=True, check=True, executable=SHELL, stdout=subprocess.PIPE, stderr=DEVNULL, encoding='ascii')
    if res.returncode == 0:
        if "statm" in cmd:
            # convert the disk, mem and vm sizes to GB
            vals= res.stdout.split()
            logres = "{now} {dbsz} {rss} {vmsz} {maps} {fds}".format(now=vals[0], dbsz=round(int(vals[1])/GB_SIZE,ROUND), rss=round(int(vals[2])*PAGE_SIZE/GB_SIZE,ROUND), vmsz=round(int(vals[3])*PAGE_SIZE/GB_SIZE,ROUND), maps=vals[4], fds=vals[5])
        else:
            logres = res.stdout

        if tee:
            print(logres)
        log.write(logres)
        return logres
    else:
        log.write("[{now}] ERROR!Failed to execute: {cmd}".format(now=mynow(), cmd=cmd))
        return None

def do_db_check(dbname, logfile):
    with open(logfile+".dbchk", 'a') as dbchklog:
        # Get the list of all tables not in a system scheme
        # result is in the format: <schema>,<table>\n
        cmd = "mclient -fcsv -s 'select s.name, t.name from schemas s, tables t where t.system = false and s.id = t.schema_id'"
        res = subprocess.run(cmd, shell=True, check=True, executable=SHELL, stdout=subprocess.PIPE, stderr=DEVNULL, encoding='ascii')
        if not res.returncode == 0:
            msg = "[{now}] ERROR!Failed to execute: {cmd}".format(now=mynow(), cmd=cmd)
            print(msg); dbchklog.write(msg)
            return None
        msg = "[{now}] {cmd} ==> found {cnt} non-system tables".format(now=mynow(), cmd=cmd, cnt=res.stdout.count("\n"))
        print(msg); dbchklog.write(msg)

        # for each user table, we do a SELECT COUNT(*)
        for s in res.stdout.split('\n'):
            if not s:
                continue
            s = s.replace(",", ".")
            cmd2 = "mclient -fcsv -s 'select count(*) from {tbl}'".format(tbl=s)
            res2 = subprocess.run(cmd2, shell=True, check=True, executable=SHELL, stdout=subprocess.PIPE, stderr=DEVNULL, encoding='ascii')
            if res2.returncode == 0:
                msg = "[{now}] {cmd} ==> {cnt}".format(now=mynow(), cmd=cmd2, cnt=res2.stdout)
            else:
                msg = "[{now}] ERROR!Failed to execute: {cmd}".format(now=mynow(), cmd=cmd2)
            print(msg); dbchklog.write(msg)


def monitor(dbpath, logfile):
    # bash command that gives: date in sec in ISO 8601 format; disksize in bytes; RSS in #pages; VM in #pages; #mmapped files; #fd
    statsCommand = """
    echo  $(date -Iseconds) $(du -bs {db_path} | cut -f1) $(cat /proc/$(pgrep mserver5)/statm | cut -d ' ' -f2) $(cat /proc/$(pgrep mserver5)/statm | cut -d ' ' -f1) $(cat /proc/$(pgrep mserver5)/maps | wc -l) $(ls -al  /proc/$(pgrep mserver5)/fd | wc -l) ;
    """.format(db_path=dbpath)

    next_maps = MMAP_INCREASE
    next_fds = FD_INCREASE
    next_dbchk = int(time.time()) + DBCHK_INTERVAL

    with open(logfile, 'a') as log:
        while True:

            # log the resource consumption info.
            res = do_log(statsCommand, log)
            if res == None:
                continue;

            # detailed log of #maps and #fds by large increases
            stats = res.split()

            # $ sysctl vm.max_map_count ==> vm.max_map_count = 65530
            # log the mmaps every 5000 or just before hitting the limit
            maps = int(stats[4])
            if maps > next_maps or maps > 65525:
                with open(logfile+".maps", 'a') as maplog:
                    mapcmd = "cat /proc/$(pgrep mserver5)/maps"
                    msg = "[{now}] {cmd}\n".format(now=mynow(), cmd=mapcmd)
                    print(msg); maplog.write(msg)
                    do_log(mapcmd, maplog, False)
                next_maps += MMAP_INCREASE

            # $ ulimit -n ==> 1024
            # log the fds every 100 or just before hitting the limit
            fd = int(stats[5])
            if fd > next_fds or fd > 1022:
                with open(logfile+".fds", 'a') as fdlog:
                    fdcmd = "ls -al /proc/$(pgrep mserver5)/fd"
                    msg = "[{now}] {cmd}\n".format(now=mynow(), cmd=fdcmd)
                    print(msg); fdlog.write(msg)
                    do_log(fdcmd, fdlog, False)
                next_fds += FD_INCREASE

            # Once in a long while, do a simple check of the database
            # Currently, count() on all user tables
            if int(time.time()) > next_dbchk:
                do_db_check(os.path.basename(dbpath), logfile)
                next_dbchk += DBCHK_INTERVAL

            time.sleep(LOG_INTERVAL)

def exit_on_error(err):
    print("ERROR!",err); print(USAGE)
    sys.exit(2)

def main(argv):
    logfile = ""
    dbname = None
    dbpath = None

    try:
        opts, args = getopt.getopt(argv, "", ["help", "dbname=", "logbase=", "log-interval=", "dbcheck-interval=", "mmap-increase=", "fd-increase=", "dbpath="])
    except getopt.GetoptError as err:
        exit_on_error(str(err))
    for opt, arg in opts:
        if opt == "--help":
            print(USAGE)
            sys.exit(0)
        elif opt == "--dbname":
            dbname = arg
        elif opt == "--dbpath":
            dbpath = arg
        elif opt == "--logbase":
            logfile = arg
        elif opt == "--log-interval":
            global LOG_INTERVAL
            LOG_INTERVAL = int(arg)
        elif opt == "--dbcheck-interval":
            global DBCHK_INTERVAL
            DBCHK_INTERVAL = int(arg)
        elif opt == "--mmap-increase":
            global MMAP_INCREASE
            MMAP_INCREASE = int(arg)
        elif opt == "--fd-increase":
            global FD_INCREASE
            FD_INCREASE = int(arg)
    if not(dbpath):
        dbpath = get_db_info(dbname)
    dbname = os.path.basename(dbpath)
    if os.path.isdir(logfile):
        os.path.join(logfile, dbname+".log")
    else:
        logfile = logfile + "_" + dbname + ".log"

    print("[{now}] DEBUG!main():monitoring {dbpath}".format(now=mynow(), dbpath=dbpath))
    print("[{now}] DEBUG!main():log files: {log}, {log}.maps, {log}.fds, {log}.dbchk\n".format(now=mynow(), log=logfile))

    monitor(dbpath, logfile)


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except KeyboardInterrupt:
        print("\nExiting ...")
        sys.exit(0)
