#!/usr/bin/env python3

import argparse
from datetime import datetime
import getopt
import glob
import os
import sys
import subprocess
import time

assert sys.version_info >= (3, 6)

# global values
USAGE = """
Monitors resource usage of one running MonetDB database:
    Timestamp DBsize(GB) RSS(GB) VMsize(GB) #mmaps #FDs
(C) 2020 MonetDB Solutions B.V.

usage: ./monitor.py [options]

--help                 Print this help screen.
--dbcheck-interval     Interval in seconds to check the data in the database,
                       default 3600.
--dbfarmcheck-interval Interval in seconds to check the files in the database,
                       default 3600.
--dbname               Name of the database to monitor.
--dbpath               Path of the database to monitor.
--fd-increase          How often to log the file descripters, default per 100
                       increases.
--logbase              Extra base filename or path for the log files,
                       default <dbname>.
--log-interval         Log interval in seconds, default 5
--mmap-increase        How often to log the memory mapped files, default per
                       5000 increases
--verbose              Log more details. Each log action implements its own.
""".strip()
SHELL = '/bin/bash'
DBCHK_INTERVAL = 3600 # sec
DBFARMCHK_INTERVAL = 3600 # sec
FD_INCREASE = 100
LOG_INTERVAL = 5 # sec
MMAP_INCREASE = 5000
VERBOSE = False

def mynow():
    return datetime.now().astimezone().replace(microsecond=0).isoformat()

def get_db_info(dbname, dbpath):
    res_name = None
    res_path = None
    res_pid  = None

    cmd = "ps aux | grep [m]server5 "
    if dbpath:
        cmd = cmd + "| grep \"" + dbpath + "\""
        dbname = os.path.basename(dbpath) 
    elif dbname:
        cmd = cmd + "| grep " + dbname

    res = None
    try:
        res = subprocess.run(cmd, shell=True, check=True, executable=SHELL,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='ascii')
    except Exception as err:
        exit_on_error("get_db_info(): failed to execute '{cmd}': {err}".format(cmd=cmd, err=str(err)))

    if res.stdout.count("mserver5") > 1:
        exit_on_error("get_db_info(): more than 1 running mserver5 process found. Please use \"--dbname\" or \"--dbpath\" to pick one:\n{info}".format(info=res.stdout))

    ress = res.stdout.split()

    # a minimal attempt to check that the 2nd field contains a possible PID
    try:
        res_pid = int(ress[1])
    except Exception as err:
        print(str(err))
        exit_on_error("get_db_info(): unexpected output of the 'ps' command: expected the 2nd field to contain the PID, got " + res_pid)

    for s in res.stdout.split():
        if "--dbpath=" == s[0:9]:
            res_path = s[9:]
            res_name = os.path.basename(res_path) 

            # This should never happen, but just some sanity check
            if dbname and res_name != dbname:
                exit_on_error("get_db_info(): expected dbname {nm1}, found {nm2}".format(nm1=dbname, nm2=res_name))
            if dbpath and res_path != dbpath:
                exit_on_error("get_db_info(): expected dbpath {pth1}, found {pth2}".format(pth1=dbpath, pth2=res_path))

            return res_name, res_path, res_pid

    if dbname:
        exit_on_error("get_db_info(): could not find database '{db}' from process info:\n{info}".format(db=dbname, info=res.stdout))
    else:
        exit_on_error("get_db_info(): could not find mserver5 from process info:\n{info}".format(info=res.stdout))

    return res_name, res_path, res_pid

def do_log(cmd, log, tee = True):
    PAGE_SIZE=4096
    GB_SIZE=1024*1024*1024

    print("[{now}] DEBUG!do_log(): execute: '{cmd}'".format(now=mynow(), cmd=cmd))

    res = None
    try:
        res = subprocess.run(cmd, shell=True, check=True, executable=SHELL,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                encoding='ascii')
    except Exception as err:
        log.write("[{now}] ERROR!Failed to execute: {cmd}: {err}\n".format(now=mynow(), cmd=cmd, err=str(err)))
        return None

    print("[{now}] DEBUG!do_log():==>\n{out}<==".format(now=mynow(), out=res.stdout))
    if "statm" in cmd:
        vals= res.stdout.split()
        # subcommands of 'cmd' can contain error messages which cause the value
        # conversions to fail
        try:
            # convert the disk, mem and vm sizes to GB
            logres = "{now} {dbsz} {rss} {vmsz} {maps} {fds}".format(
                now=vals[0],
                dbsz=round(int(vals[1])/GB_SIZE, 2),
                rss=round(int(vals[2])*PAGE_SIZE/GB_SIZE, 2),
                vmsz=round(int(vals[3])*PAGE_SIZE/GB_SIZE, 2),
                maps=vals[4],
                fds=vals[5])
        except Exception as err:
            log.write("[{now}] ERROR!Failed to execute: {cmd}=>{out}<=|=>{err}<=\n".format(now=mynow(), cmd=cmd, out=str(res.stdout), err=str(res.stderr)))
            return None
    else:
        logres = res.stdout

    if tee:
        print(logres)
    log.write(logres + "\n")
    return logres

# Check the data in the database.
# Currently, run a count(*) on all user tables
def do_db_check(dbname, logfile):
    #FIXME: use pymonetdb

    with open(logfile, 'a') as dbchklog:
        # Get the count of all tables not in a system scheme
        # result is in the format: <schema>,<table>,<cnt>,<cnt_ok>\n
        cmd = "mclient -d {dbname} -fcsv -s '" \
        "select s.schema, s.table, min(count) as cnt, " \
        "       min(count) = max(count) as cnt_ok " \
        "from sys.storage() s, " \
        "    (select s.name as schema, t.name as table " \
        "     from schemas s, tables t " \
        "     where t.system = false and s.id = t.schema_id) t " \
        "where s.schema = t.schema and s.table = t.table " \
        "group by s.schema, s.table;'".format(dbname=dbname)

        res = None
        try:
            res = subprocess.run(cmd, shell=True, check=True, executable=SHELL,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    encoding='ascii')
        except Exception as err:
            msg = "[{now}] ERROR!Failed to execute: {cmd}: {err}".format(now=mynow(), cmd=cmd, err=str(err))
            print(msg); dbchklog.write(msg + "\n")
            return None

        msg = "[{now}] {cmd} ==> found {cnt} non-system tables".format(now=mynow(), cmd=cmd, cnt=res.stdout.count("\n"))
        print(msg); dbchklog.write(msg + "\n")

        # for each user table, we print its count
        for s in res.stdout.split('\n'):
            # check for end of the CSV
            if not s:
                continue

            ss = s.split(",")
            if len(ss) != 4:
                msg = "[{now}] ERROR!unexpected result for DB table count: {res}".format()
                print(msg); dbchklog.write(msg + "\n")
                return

            msg = "[{now}] COUNT(*) FROM {schema}.{tbl} ==> {cnt} (ok = {ok})".format(now=mynow(), schema=ss[0], tbl=ss[1], cnt=ss[2], ok=ss[3])
            print(msg); dbchklog.write(msg + "\n")

# Check the files in the dbfarm.
# Current, count the files with these extensions:
# .tail, .theap, .thashl, .thashb, .thsh*, .timprints, .torderidx
def do_dbfarm_check(dbpath, logfile):
    # FIXME: Might need to change the implementation here, since using the “**”
    #        pattern in large directory trees may cause performance problems.
    exts = (
            "tail",
            "theap",
            "thashl",
            "thashb",
            "thsh*",
            "timprints",
            "torderidx",
            "total") # always end with "total"
    msg = "[{now}] ".format(now=mynow())
    for f in exts:
        if f != 'total':
            cnt = len(glob.glob(dbpath+'/**/*.'+f, recursive=True))
        else:
            cnt = len(glob.glob(dbpath+'/**', recursive=True))
        msg += "{name}:{cnt} ".format(name=f, cnt=cnt)

    with open(logfile, 'a') as dbfarm_chklog:
        print(msg); dbfarm_chklog.write(msg + "\n")

def monitor(dbname, dbpath, dbpid, logbase):
    # FIXME: replace as many as possible the SHELL commands with Python
    #        functions

    # TODO: put every log in a subthread/subprocess

    # bash command that gives:
    #   date in sec in ISO 8601 format;
    #   disksize in bytes;
    #   RSS in #pages;
    #   VM in #pages;
    #   number of mmapped files;
    #   number of FDs
    statsCommand = "echo " \
    "$(date -Iseconds) " \
    "$(du -bs {db_path} | cut -f1) " \
    "$(cat /proc/{db_pid}/statm | cut -d ' ' -f2) " \
    "$(cat /proc/{db_pid}/statm | cut -d ' ' -f1) " \
    "$(cat /proc/{db_pid}/maps | wc -l) " \
    "$(ls -al  /proc/{db_pid}/fd | wc -l) ;".format(db_path=dbpath, db_pid=dbpid)

    # only log these values if their counts are high enough
    next_maps = MMAP_INCREASE
    next_fds = FD_INCREASE
    # let's do these checks immediately
    next_dbchk = next_dbfarmchk = int(time.time()) - 1

    with open(logbase+".log", 'a') as log:
        while True:

            # log the resource consumption info.
            res = do_log(statsCommand, log)
            log.flush()
            if res == None:
                continue;

            # detailed log of #maps and #fds by large increases
            stats = res.split()

            # $ sysctl vm.max_map_count ==> vm.max_map_count = 65530
            # log the mmaps every 5000 or just before hitting the limit
            maps = int(stats[4])
            if maps > next_maps or maps > 65525:
                with open(logbase+"_maps.log", 'a') as maplog:
                    mapcmd = "cat /proc/{dbpid}/maps".format(dbpid=dbpid)
                    msg = "[{now}] {cmd}\n".format(now=mynow(), cmd=mapcmd)
                    print(msg); maplog.write(msg + "\n")
                    do_log(mapcmd, maplog, False)
                next_maps += MMAP_INCREASE

            # $ ulimit -n ==> 1024
            # log the fds every 100 or just before hitting the limit
            fd = int(stats[5])
            if fd > next_fds or fd > 1022:
                with open(logbase+"_fds.log", 'a') as fdlog:
                    fdcmd = "ls -al /proc/dbpid/fd".format(dbpid=dbpid)
                    msg = "[{now}] {cmd}\n".format(now=mynow(), cmd=fdcmd)
                    print(msg); fdlog.write(msg + "\n")
                    do_log(fdcmd, fdlog, False)
                next_fds += FD_INCREASE

            # Once in a long while, check the data in the database and the
            # files in the dbfarm
            if int(time.time()) > next_dbchk:
                do_db_check(os.path.basename(dbpath), logbase+"_dbchk.log")
                next_dbchk += DBCHK_INTERVAL
            if int(time.time()) > next_dbfarmchk:
                do_dbfarm_check(dbpath, logbase+"_dbfarmchk.log")
                next_dbfarmchk += DBFARMCHK_INTERVAL

            time.sleep(LOG_INTERVAL)

def exit_on_error(err):
    print("ERROR!",err)
    print(USAGE)
    sys.exit(2)

def main(argv):
    logbase = ""
    dbname = None
    dbpath = None
    dbpid  = None

    # TODO: maybe a better way to parse the commandline options, e.g. to have
    # option help messages and the option parser in one place
    try:
        opts, args = getopt.getopt(argv, "", [
            "help",
            "dbcheck-interval=",
            "dbfarmcheck-interval=",
            "dbname=",
            "dbpath=",
            "dbpid=",
            "fd-increase=",
            "logbase=",
            "log-interval=",
            "mmap-increase=",
            "verbose="
            ])
    except getopt.GetoptError as err:
        exit_on_error(str(err))

    for opt, arg in opts:
        if opt == "--help":
            print(USAGE)
            sys.exit(0)
        elif opt == "--dbcheck-interval":
            global DBCHK_INTERVAL
            DBCHK_INTERVAL = int(arg)
        elif opt == "--dbfarmcheck-interval":
            global DBFARMCHK_INTERVAL
            DBFARMCHK_INTERVAL = int(arg)
        elif opt == "--dbname":
            dbname = arg
        elif opt == "--dbpath":
            dbpath = arg
        elif opt == "--fd-increase":
            global FD_INCREASE
            FD_INCREASE = int(arg)
        elif opt == "--logbase":
            logbase = arg
        elif opt == "--log-interval":
            global LOG_INTERVAL
            LOG_INTERVAL = int(arg)
        elif opt == "--mmap-increase":
            global MMAP_INCREASE
            MMAP_INCREASE = int(arg)
        elif opt == "--verbose":
            global VERBOSE
            VERBOSE = True

    if (dbname and dbpath) and (dbname != os.path.basename(dbpath)):
        exit_on_error("\--dbname\" and \--dbpath\" contain different database names")

    dbname, dbpath, dbpid = get_db_info(dbname, dbpath)


    try:
        subprocess.run("which mclient", shell=True, check=True,
                executable=SHELL)
    except Exception as err:
        print("ERROR!Could not find 'mclient' in $PATH")
        sys.exit(2)

    if os.path.isdir(logbase):
        os.path.join(logbase, dbname)
    else:
        logbase = logbase + "_" + dbname

    print("[{now}] DEBUG!main(): monitoring {dbpath}".format(now=mynow(), dbpath=dbpath))
    print("[{now}] DEBUG!main(): log files: " \
            "{log}.log, " \
            "{log}_fds.log, " \
            "{log}_dbchk.log, " \
            "{log}_files.log, " \
            "{log}_maps.log\n".format(now=mynow(), log=logbase))

    monitor(dbname, dbpath, dbpid, logbase)


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except KeyboardInterrupt:
        print("\nExiting ...")
        sys.exit(0)
