<!-- ![Logo](https://user-images.githubusercontent.com/32074414/111834321-cf245180-88c9-11eb-9862-2c83cb527ff6.png) -->

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
 * Average R/W Requests per sec (Cluster/Table)
 * Average R/W Requests per Month (Cluster)
 * Percentage of R/W Requests of total R,W and RW load (Table)
 * Percentage of R/W Requests of total RW load (Cluster) 

Data Size
 * Data Set Size (Cluster/Table)

GC Pauses
 * Count (Cluster/DC/Node)
 * Max/Min/P99/P98/P95/P75/P50 (Cluster/DC/Node)
 * From/To/Max Date/Time

Node Data
 * Load/Tokens/Rack

Node R/W Latency Proxihistogram
 * P99/P98/P95/P75/P50

A list of the following above a test parameter
 * Dropped Mutations (Node/Keyspace/Table)
 * Table Count per DC
 * Large Partitions
 * SSTable Count
 * Read Latency (over 5ms)
 * Write Latency (over 1ms)
 * Tombstones

<!-- ORIGINS OF THE CODE -->
## Origins of the Code
This code was created to assist in identifying write and read calls per month for use of estimating Astra usage costs.  For so long, the max tps ruled everything.  Environments were built on the daily, weekly or monthly max loads.  Now that there is a Cassandra DBaaS - DataStax Astra (https://astra.datastax.com) with prices based on averages, the nessesity to get average transaction numbers is important. Enjoy!! 

The Astra-Perseverance was appropriatelly named after the NASA Mars Rover - Perseverance.  It's purpose is to gather and communicate information about Mars.   

<!-- ITEMS ANALYZED -->
## Items Analyzed
The following items are analzed for potential issues with migration to Astra. The test parameters may be changed with arguments in the command line.

### Astra Guardrails
 * Number of materialized views per table
   - Guardrail limit: 2
   - Test Parameter: 2
 * Number of secondary indexes per table
   - Guardrail limit: 1
   - Test Parameter: 1
 * Number of storage-attached indexes per table
   - Guardrail limit: 10
   - Test Parameter: 8
 * Number of tables in a keyspace
   - Guardrail limit: 200
   - Test Parameter: 175
 * Number of fields in a table
   - Guardrail limit: 50
   - Test Parameter: 45
 * Large partition size (MB)
   - Guardrail limit: 200
   - Test Parameter: 100
 * Use of UDA and UDF

### Cluster Health
 * Local read latency (ms)
   - Test Parameter: 100
 * Local write latency (ms)
   - Test Parameter: 100
 * Node P99 GC pause time (ms)
   - Test Parameter: 800
 * Number of dropped mutations per table
   - Test Parameter: 100000

<!-- GETTING STARTED -->
## Getting Started
Install XlsxWriter (https://xlsxwriter.readthedocs.io/getting_started.html)

### Gather Diagnostic Data
After cloning this project, gather diagnostic data from the cluster

#### Using DSE OpsCenter
Download a diagnostic tarball from a targeted Cassandra cluster through DSE OpsCenter

#### Using Diagnostic-Collection Tool
Collect diagnostic tarball using the Cassandra Diagnostic Collection Tool - https://github.com/datastax-toolkit/diagnostic-collection. 
Note: If you are using the Cassandra Diagnostic Collection tool, it is easiest to collect a complete cluster diag tarball at once using:
```
./collect_diag.sh -t dse -f mhosts -r -s \
  "-i ~/.ssh/private_key -o StrictHostKeyChecking=no -o User=automaton"
```
or for open source cassandra:
```
./collect_diag.sh -t coss -f mhosts -r -s \
  "-i ~/.ssh/private_key -o StrictHostKeyChecking=no -o User=automaton"
```
mhost is a file with a list of nodes (one per line)

#### Manually Collect and Create Diagnostic Zip File
Collect the following files and add the files into the file structure below.
 - [...]/driver/schema
 - [...]/logs/cassandra/system.log
 - [...]/java_system_properties.json
  or
 - [...]/java_system_properties.txt

Run the following nodetool commands on each node and add the output into the file structure below.
 - nodetool cfstats > cfstats
 - nodetool info > info
 - nodetool describecluster > describecluster
 - nodetool gossipinfo > gossipinfo
 - nodetool status > status
 - nodetool version > version
 - nodetool proxyhistograms > proxyhistograms

```
[Cluster_Name]
  nodes
    [ipaddress]
      nodetool
        cfsats
        info
        describecluster
        gossipinfo
        status
        version
        proxyhistograms
      driver
        schema
      logs
        cassandra
          system.log
      java_system_properties.json (or java_system_properties.txt)
```

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
#### Changing Test Parameters
```
-tp_tblcnt             Database Table Count (Guardrail)
                        Number of tables in the database
                        to be listed in the Number of Tables tab
                        Astra Guardrail Limit: 200
                        Test Parameter: >175
-tp_colcnt             Table Column Count (Guardrail)
                        Number of columns in a table
                        Astra Guardrail Limit: 50
                        Test Parameter: >45
-tp_mv                 Materialized Views  (Guardrail)
                        Number of Materialized Views of a table
                        Astra Guardrail Limit: 2
                        Test Parameter: >2
-tp_si                Secondary Indexes  (Guardrail)
                        Number of Secondary Indexes of a table
                        Astra Guardrail Limit: 1
                        Test Parameter: >1
-tp_sai                Storage Attached Indexes  (Guardrail)
                        Number of SAI of a table
                        Astra Guardrail Limit: 10
                        Test Parameter: >8
-tp_lpar               Large Partitions (Guardrail)
                        Size of partition in MB
                        to be listed in the Large Partition tab
                        Astra Guardrail Limit: 200MB
                        Test Parameter: >100
-tp_rl                 Local Read Latency (Database Health)
                        Local read time(ms) in the cfstats log
                        to be listed in the Read Latency tab
                        Test Parameter: >100
-tp_wl                 Local Write Latency (Database Health)
                        Local write time(ms) in the cfstats log
                        to be listed in the Read Latency tab
                        Test Parameter: >100
-tp_sstbl              SSTable Count (Database Health)
                        SStable count in the cfstats log
                        to be listed in the Table Qantity tab
                        Test Parameter: >20
-tp_drm                Dropped Mutations (Database Health)
                        Dropped Mutation count in the cfstats log
                        to be listed in the Dropped Mutation tab
                        Test Parameter: >100000

-tp_gcp                GCPauses (Database Health)
                        Node P99 GC pause time (ms)
                        to be listed in the GC Pauses tab
                        Test Parameter: >800
```
Notice: Test parameters cannot be larger than guardrails

#### Help
There is a help section:
```
python explore.py --help
```
