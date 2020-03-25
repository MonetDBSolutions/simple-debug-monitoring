# simple-debug-monitoring

Probably works on all Linux systems with bash.
Make sure you have installed matplotlib and all of its dependencies.

Just run 

```bash
./plot.py <db_path>
```

It should pop up a matplotlib display with three graphs: 
* virtual memory usage
* resident set size
* database disk footprint.

The script asumes there is a single mserver5 running.

Set `<db_path>` to a valid relative or absolute path pointing to the root of the database, i.e. what is passed to --db_path parameter.
