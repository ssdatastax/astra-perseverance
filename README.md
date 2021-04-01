![Logo](https://user-images.githubusercontent.com/32074414/111834321-cf245180-88c9-11eb-9862-2c83cb527ff6.png)

<!-- TABLE OF CONTENTS -->
## Table of Contents

* [Summary](#summary)
* [Script Origins](#origins-of-the-code)
* [Items Analyzed](#items-analyzed)
* [Getting Started](#getting-started)
* [Creating the Spreadsheet](#using-the-cluster-load-spreadsheet)

<!-- SUMMARY -->
## Summary
Astra Perseverance is a tool to analyze a cassandra cluster to gain an initial insight on migrating cassandra clusters to DataStax Astra, the Cassandra DBAAS.

This script extracts and organizes key information from diagnostic files from the cluster related to migrating a cassandra cluster to Astra. The following information is organized in a spreadsheet with multiple tabs:

Workload 
 * Average R/W Requests Transactions per sec (TPS) (Cluster/Table)
 * Average R/W Requests Transactions per Month (TPMO) (Cluster)
 * Percentage of R/W Requests of total R,W and RW load (Table)
 * Percentage of R/W Requests of total RW load (Cluster)
 * RF (Keyspace/Table) 

Data Size
 * Data Size (Cluster/Table)
 * RF (Keyspace/Table)
 * Data Set Size (Cluster/Table)

GC Pauses
 * Count (Cluster/DC/Node)
 * Max/Min/P99/P98/P95/P75/P50 (Cluster/DC/Node)
 * From/To/Max Date/Time

Node Data
 * Load/Tokens/Rack

Node R/W Latency Proxihistogram
 * P99/P98/P95/P75/P50

A list of the following above a threshhold value
 * Dropped Mutations (Node/Keyspace/Table)
 * Table Count per DC;
 * Large Partitions;
 * SSTable Count;
 * Read Latency (over 5ms);
 * Write Latency (over 1ms);
 * Tombstones (Future version)


<!-- ORIGINS OF THE CODE -->
## Origins of the Code
This code was created to assist in identifying write and read calls per month for use of estimating Astra usage costs.  For so long, the max tps ruled everything.  Environments were built on the daily, weekly or monthly max loads.  Now that there is a Cassandra DBaaS - DataStax Astra (https://astra.datastax.com) with prices based on averages, the nessesity to get average transaction numbers is important. Enjoy!! 

The Astra-Perseverance was appropriatelly named after the NASA Mars Rover - Perseverance.  It's purpose is to gather and communicate information about Mars.   

<!-- ITEMS ANALYZED -->
## Items Analyzed
The following items are analzed for potential issues with migration to Astra:

Astra Guardrails

- Number of materialized views per table
 - Number of indexes per table
 - Number of custom indexes per table
 - Number of tables in a keyspace
 - Number of fields in a table
 - Partition size (MB)
 - Use of UDA and UDF

Cluster Health

- Node read latency (ms)
 - Node write latency (ms)
 - Node P99 GC pause time
 - Number of dropped mutations per node/table


<!-- GETTING STARTED -->
## Getting Started
Install XlsxWriter (https://xlsxwriter.readthedocs.io/getting_started.html)

After cloning this project, download a diagnostic tarball from a targeted Cassandra cluster through DSE OpsCenter or using the Cassandra Diagnostic Collection Tool - https://github.com/datastax-toolkit/diagnostic-collection. 
Note: If you are using the Cassandra Diagnostic Collection tool, it is easiest to collect a complete cluster diag tarball at once using:
```
./collect_diag.sh -t dse -f mhosts -r -s \
  "-i ~/.ssh/private_key -o StrictHostKeyChecking=no -o User=automaton"
```
or for open source cassandra:
```
./collect_diag.sh -t oss -f mhosts -r -s \
  "-i ~/.ssh/private_key -o StrictHostKeyChecking=no -o User=automaton"
```
mhost is a file with a list of nodes (one per line)

### Commands and Arguments

#### Creating the Spreadsheet
To create the spreadsheet run the following command:
```
python explore.py -p [path_to_diag_folder]
```
You may run the script on multiple diagnostic folders:
```
python explore.py -p [path_to_diag_folder1] -p [path_to_diag_folder2] -p [path_to_diag_folder3]
```

#### Help
There is a brief help info section:
```
python explore.py --help
``` 
