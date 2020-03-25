#!/usr/bin/env bash


gather () {
    #echo $(date +"%u%H%M%S") $(du -s ../monetdb/devdb | cut -f1) $(du -s ../monetdb/devdb/sql_logs/sql/ | cut -f1) $(cat /proc/$(pgrep mserver5)/statm | cut -d ' ' -f2);
    #echo $(date +"%u%H%M%S");
    #echo $(date --iso-8601=seconds) $(du -s ../monetdb/devdb | cut -f1);
    #echo $(date +"%Y-%m-%dT%T") $(du -s ../monetdb/devdb | cut -f1) $(cat /proc/$(pgrep callgrind-amd64)/statm | cut -d ' ' -f2);
    echo $(date +"%Y-%m-%dT%T") $(du -s ../clone_monetdb/devdb | cut -f1) $(cat /proc/$(pgrep mserver5)/statm | cut -d ' ' -f2) $(cat /proc/$(pgrep mserver5)/statm | cut -d ' ' -f1);
    #echo $(date +"%Y-%m-%dT%T") $(du -s ../clone_monetdb/devdb | cut -f1) $(cat /proc/$(pgrep mserver5)/statm | cut -d ' ' -f2);
}



while true; do
gather | tee -a trace.dat pipe.dat
#gather | tee -a trace.dat
sleep 1;
done;
