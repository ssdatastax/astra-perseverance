![Logo](https://user-images.githubusercontent.com/32074414/111834321-cf245180-88c9-11eb-9862-2c83cb527ff6.png)

<!-- TABLE OF CONTENTS -->
## Table of Contents

* [Summary](#summary)
* [Script Origins](#origins-of-the-code)
* [Getting Started](#getting-started)
* [Creating the Spreadsheet](#using-the-cluster-load-spreadsheet)

<!-- SUMMARY -->
## Summary
Astra Perseverance is a tool to analyze a cassandra cluster to gain an initial insight on moving the cluster to DataStax Astra, the Cassandra DBAAS.

This script extracts and organizes key information from diagnostic files from the cluster related to migrating a cassandra cluster to Astra. The following information is organized in a spreadsheet with multiple tabs:

Workload 
 * R/W TPS (Cluster/Table)
 * R/W TPMO (Cluster)
 * Percentage of R/W of total R,W and RW load (Table)
 * Percentage of R/W of total RW load (Cluster)
 * RF (Keyspace/Table) 

Data Size
 * Data Size (Cluster/Table)
 * RF (Keyspace/Table)
 * Data Set Size (Cluster/Table)
 * Estimated Average Row Size (Table)
 * Estimated Row Rount (Table)

GC Pauses
 * Count (Cluster/DC/Node)
 * Max/Min/P99/P98/P95/P75/P50 (Cluster/DC/Node)
 * From/To/Max Date/Time

Node Data
 * Load/Tokens/Rack

Node R/W Latency Proxihistogram
 * P99/P98/P95/P75/P50

Dropped Mutations
 * Count (Node/DC/Keyspace/Table)

Table Count per DC;
Wide Partitions (over 100 MB);
SSTable Count (over 15);
Read Latency (over 5ms);
Write LAtency (over 1ms);
Tombstones (Future version)



<!-- ORIGINS OF THE CODE -->
## Origins of the Code
This code was created to assist in identifying write and read calls per month for use of estimating Astra usage costs.  For so long, the max tps ruled everything.  Environments were built on the daily, weekly or monthly max loads.  Now that there is a Cassandra DBaaS - DataStax Astra (https://astra.datastax.com) with prices based on averages, the nessesity to get average transaction numbers is important. Enjoy!! 

The Astra-Perseverance was appropriatelly named after the NASA Mars Rover - Perseverance.  It's purpose is to gather and communicate information about Mars.   

<!-- GETTING STARTED -->
## Getting Started
Install xlsxwriter and Pandas

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
python look.py -p [path_to_diag_folder]
```
You may run the script on multiple diagnostic folders:
```
python look.py -p [path_to_diag_folder1] -p [path_to_diag_folder2] -p [path_to_diag_folder3]
```

#### Help
There is a brief help info section:
```
python extract_load.py --help
``` 
