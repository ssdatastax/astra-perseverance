#!/usr/bin/env python3

#pip install xlsxwriter
#pip install Pandas

import os.path
from os import path
import xlsxwriter
import sys
import pandas as pd
import datetime
import re
import zipfile

# write comment on worksheet field
def write_cmt(wksht,coord,title,vis=0):
  for cmt_array in comments:
    if title in cmt_array['fields']:
      wksht.write_comment(coord,cmt_array['comment'][0],{'visible':vis,'font_size': 12,'x_scale': 2,'y_scale': 2})

# collect the dc name for each node
def get_dc(statuspath):
  if(path.exists(statuspath)):
    statusFile = open(statuspath, 'r')
    dc = ''
    node = ''
    for line in statusFile:
      if('Datacenter:' in line):
        dc = str(line.split(':')[1].strip())
        if dc not in dc_array:
          dc_array.append(dc)
        dc_gcpause[dc]=[]
        newest_gc[dc]={'jd':0.0,'dt':''}
        oldest_gc[dc]={'jd':99999999999.9,'dt':''}
        max_gc[dc]=''
      elif(line.count('.')>=3):
        node = str(line.split()[1].strip())
        node_dc[node] = dc
  else:
    exclude_tab.append('node')

# collect GC info from system.log
def parseGC(node,systemlog,systemlogpath):
  if(zipfile.is_zipfile(systemlog)):
    zf = zipfile.ZipFile(systemlog, 'r')
    systemlogFile = zf.read(zf.namelist()[0])
  else:
    systemlogFile = open(systemlog, 'r')
  for line in systemlogFile:
    if('GCInspector.java:' in line):
      if(line.split()[2].strip().count('-')==2): date_pos=2
      else: date_pos=3
      dt = line.split()[date_pos].strip()
      tm = line.split()[date_pos+1].split(',')[0].strip()
      dc = node_dc[node]
      gcpause = line[line.find('GC in')+6:line.find('ms.')]
      ldatetime = dt + ' ' + re.sub(',.*$','',tm.strip())
      log_dt = datetime.datetime.strptime(ldatetime,log_df)
      log_jd = pd.Timestamp(year=log_dt.year,month=log_dt.month,day=log_dt.day,hour=log_dt.hour,minute=log_dt.minute,tz=tz[node]).to_julian_date()
      cluster_gcpause.append(int(gcpause))
      dc_gcpause[dc].append(int(gcpause))
      node_gcpause[node].append(int(gcpause))
      if(newest_gc[cluster_name]['jd']<log_jd): newest_gc[cluster_name]={'jd':log_jd,'dt':ldatetime + ' ' + tz[node]}
      if(oldest_gc[cluster_name]['jd']>log_jd): oldest_gc[cluster_name]={'jd':log_jd,'dt':ldatetime + ' ' + tz[node]}
      if(newest_gc[dc]['jd']<log_jd): newest_gc[dc]={'jd':log_jd,'dt':ldatetime + ' ' + tz[node]}
      if(oldest_gc[dc]['jd']>log_jd): oldest_gc[dc]={'jd':log_jd,'dt':ldatetime + ' ' + tz[node]}
      if(newest_gc[node]['jd']<log_jd): newest_gc[node]={'jd':log_jd,'dt':ldatetime + ' ' + tz[node]}
      if(oldest_gc[node]['jd']>log_jd): oldest_gc[node]={'jd':log_jd,'dt':ldatetime + ' ' + tz[node]}
      if(max(node_gcpause[node])==int(gcpause)): max_gc[node]= ldatetime

# organize the GC pauses into percentage
def get_gc_data(level,name,gcpause,is_node):
    gcpause.sort()
    gcnum=len(gcpause)
    min_pos = 0
    p50_pos = int(round(gcnum*.5)-1)
    p75_pos = int(round(gcnum*.75)-1)
    p90_pos = int(round(gcnum*.90)-1)
    p95_pos = int(round(gcnum*.95)-1)
    p98_pos = int(round(gcnum*.98)-1)
    p99_pos = int(round(gcnum*.99)-1)
    max_pos = int(gcnum-1)
    gc_data[name] = {'Level':str(level)}
    gc_data[name].update({'Name':str(name)})
    gc_data[name].update({'Pauses':str(gcnum)})
    if(gcnum):
        gc_data[name].update({'Min':str(gcpause[min_pos])})
        gc_data[name].update({'P50':str(gcpause[p50_pos])})
        gc_data[name].update({'P75':str(gcpause[p75_pos])})
        gc_data[name].update({'P90':str(gcpause[p90_pos])})
        gc_data[name].update({'P95':str(gcpause[p95_pos])})
        gc_data[name].update({'P98':str(gcpause[p98_pos])})
        gc_data[name].update({'P99':str(gcpause[p99_pos])})
        gc_data[name].update({'Max':str(gcpause[max_pos])})
    else:
      gc_data[name].update({'Min':'N/A'})
      gc_data[name].update({'P50':'N/A'})
      gc_data[name].update({'P75':'N/A'})
      gc_data[name].update({'P90':'N/A'})
      gc_data[name].update({'P95':'N/A'})
      gc_data[name].update({'P98':'N/A'})
      gc_data[name].update({'P99':'N/A'})
      gc_data[name].update({'Max':'N/A'})

# sort array
def sortFunc(e):
  return e['count']

# write data on spreadsheet
def write_row(sheet_name,row_data,d_format,blank_col=[]):
  for col_num,data in enumerate(row_data):
    if col_num not in blank_col:
      stats_sheets[sheet_name].write(row[sheet_name],col_num, data, d_format)
  row[sheet_name]+=1

# collect targeted value in a log file
def get_param(filepath,param_name,param_pos,ignore='',default_val='Default'):
  if(path.exists(filepath)):
    fileData = open(filepath, 'r')
    for line in fileData:
      if(param_name in line):
        if(ignore):
          if((ignore in line and line.find(ignore)>0) or ignore not in line):
            default_val = str(line.split()[param_pos].strip())
        else:
          if(str(line.split()[param_pos].strip())):
            def_val = str(line.split()[param_pos].strip())
          return def_val
  else:
    exit('ERROR: No File: ' + filepath)

# initialize script variables
data_url = []
read_threshold = 1
write_threshold = 1
new_dc = ''
show_help = ''
include_system = 0
log_df = '%Y-%m-%d %H:%M:%S'
dt_fmt = '%m/%d/%Y %I:%M%p'
tz = {}
th_rl = 5
th_wl = 1
th_sstbl = 15
th_gcp = 800
th_drm = 100000
th_tblcnt = 100
th_wpar = 100

# communicate command line help
for argnum,arg in enumerate(sys.argv):
  if(arg=='-h' or arg =='--help'):
    help_content = \
      'usage: look.py [-h] [--help] [-inc_yaml]\n'\
      '                       [-p PATH_TO_DIAG_FOLDER]\n'\
      '                       [-th_rl READ_LATENCY_THRESHOLD]\n'\
      '                       [-th_wl WRITE_LATENCY_THRESHOLD]\n'\
      '                       [-th_sstbl SSTABLE_COUNT_THRESHOLD]\n'\
      '                       [-th_drm DROPPED_MUTATIONS_COUNT_THRESHOLD]\n'\
      '                       [-th_tblcnt CLUSTER_TABLE_COUNT_THRESHOLD]\n'\
      '                       [-th_wpar WIDE_PARTITON_SIZE_THRESHOLD]\n'\
      'required arguments:\n'\
      '-p                     Path to the diagnostics folder\n'\
      '                        Multiple diag folders accepted\n'\
      '                        i.e. -p PATH1 -p PATH2 -p PATH3\n'\
      'optional arguments:\n'\
      '-h, --help             This help info\n'\
      '-th_rl                 Threshold: Read Latency\n'\
      '                        Local read time(ms) in the cfstats log \n'\
      '                        to be listed in the Read Latency tab\n'\
      '                        Default Value: '+str(th_rl)+'\n'\
      '-th_wl                 Threshold: Write Latency\n'\
      '                        Local write time(ms) in the cfstats log \n'\
      '                        to be listed in the Read Latency tab\n'\
      '                        Default Value: '+str(th_wl)+'\n'\
      '-th_sstbl              Threshold: SSTable Count\n'\
      '                        SStable count in the cfstats log \n'\
      '                        to be listed in the Table Qantity tab\n'\
      '                        Default Value: '+str(th_sstbl)+'\n'\
      '-th_drm                Threshold: Dropped Mutations\n'\
      '                        Dropped Mutation count in the cfstats log \n'\
      '                        to be listed in the Dropped Mutation tab\n'\
      '                        Default Value: '+str(th_drm)+'\n'\
      '-th_tblcnt             Threshold: Cluster Table Count\n'\
      '                        Quantity of tables in the cluster\n'\
      '                        to be listed in the Table Qty tab\n'\
      '                        Default Value: '+str(th_tblcnt)+'\n'\
      '-th_wpar               Threshold: Wide Partitions\n'\
      '                        Size of partition (MB)\n'\
      '                        to be listed in the Wide Partition tab\n'\
      '                        Default Value: '+str(th_wpar)+'\n'
    
    exit(help_content)

# collect and analyze command line arguments
for argnum,arg in enumerate(sys.argv):
  if(arg=='-p'):
    data_url.append(sys.argv[argnum+1])
  elif(arg=='-th_rl'):
    th_rl = float(sys.argv[argnum+1])
  elif(arg=='-th_wl'):
    th_wl = float(sys.argv[argnum+1])
  elif(arg=='-th_sstbl'):
    th_sstbl = float(sys.argv[argnum+1])
  elif(arg=='-th_drm'):
    th_drm = float(sys.argv[argnum+1])
  elif(arg=='-th_tblcnt'):
    th_tblcnt = float(sys.argv[argnum+1])
  elif(arg=='-th_wpar'):
    th_wpar = float(sys.argv[argnum+1])

# Organize primary support tab information
sheets_data = []
sheets_data.append({'sheet_name':'node','tab_name':'Node Data','freeze_row':1,'freeze_col':0,'cfstat_filter':'','headers':['Node','DC','Load','Tokens','Rack'],'widths':[18,14,14,8,11],'extra':0,'comment':''})
sheets_data.append({'sheet_name':'ph','tab_name':'Proxihistogram','freeze_row':2,'freeze_col':0,'cfstat_filter':'','headers':['Node','P99','P98','95%','P75','P50','','Node','P99','P98','95%','P75','P50'],'widths':[18,5,5,5,5,5,3,18,5,5,5,5,5],'extra':0,'comment':''})
sheets_data.append({'sheet_name':'dmutation','tab_name':'Dropped Mutation','freeze_row':1,'freeze_col':0,'cfstat_filter':'Dropped Mutations','headers':['Node','DC','Keyspace','Table','Dropped Mutations'],'widths':[18,14,14,25,20],'filter_type':'>=','filter':th_drm,'strip':'','extra':0,'comment':'Tables with more than '+str(th_drm)+' dropped mutations (cfstats)'})
sheets_data.append({'sheet_name':'numTables','tab_name':'Table Qty','freeze_row':1,'freeze_col':0,'cfstat_filter':'Total number of tables','headers':['Node','DC','Keyspace','Table','Total Number of Tables'],'widths':[18,14,14,25,23],'filter_type':'>=','filter':th_tblcnt,'strip':'','extra':0,'comment':''})
sheets_data.append({'sheet_name':'partition','tab_name':'Wide Partitions','freeze_row':1,'freeze_col':0,'cfstat_filter':'Compacted partition maximum bytes','headers':['Example Node','DC','Keyspace','Table','Partition Size(MB)'],'widths':[18,14,14,25,18],'filter_type':'>=','filter':th_wpar*1000000,'strip':'','extra':1,'comment':'Table with partiton sizes greater than '+str(th_wpar)+' (cfstats)'})
sheets_data.append({'sheet_name':'sstable','tab_name':'SSTable Count','freeze_row':1,'freeze_col':0,'cfstat_filter':'SSTable count','headers':['Example Node','DC','Keyspace','Table','SSTable Count'],'widths':[18,14,14,25,15],'filter_type':'>=','filter':th_sstbl,'strip':'','extra':1,'comment':''})
sheets_data.append({'sheet_name':'rlatency','tab_name':'Read Latency','freeze_row':1,'freeze_col':0,'cfstat_filter':'Local read latency','headers':['Node','DC','Keyspace','Table','Read Latency (ms)'],'widths':[18,14,14,25,20],'filter_type':'>=','filter':th_rl,'strip':'ms','extra':0,'comment':''})
sheets_data.append({'sheet_name':'wlatency','tab_name':'Write Latency','freeze_row':1,'freeze_col':0,'cfstat_filter':'Local write latency','headers':['Node','DC','Keyspace','Table','Write Latency (ms)'],'widths':[18,14,14,25,20],'filter_type':'>=','filter':th_wl,'strip':'ms','extra':0,'comment':''})
#sheets_data.append({'sheet_name':'ts','tab_name':'Tombstones','headers':['Node','DC','Keyspace','Table','Write Latency (ms)'],'extra':0})

system_keyspace = ['OpsCenter','dse_insights_local','solr_admin','test','dse_system','dse_analytics','system_auth','system_traces','system','dse_system_local','system_distributed','system_schema','dse_perf','dse_insights','dse_security','dse_system','killrvideo','dse_leases','dsefs_c4z','HiveMetaStore','dse_analytics','dsefs','spark_system']
ks_type_abbr = {'app':'Application','sys':'System'}

comments = [
{
'fields':['Data Size (GB)','Data Set Size'],
'comment':["Data Size is a single set of complete data.  It does not include replication data across the cluster"]
},{
'fields':['Read Requests'],
'comment':["The number of read requests on the coordinator nodes during the nodes uptime, analogous to client writes."]
},{
'fields':['Write Requests'],
'comment':["The number of write requests on the coordinator nodes during the nodes uptime, analogous to client writes."]
},{
'fields':['% Reads'],
'comment':["The table's pecentage of the total read requests in the cluster."]
},{
'fields':['% Writes'],
'comment':["The table's pecentage of the total write requests in the cluster."]
},{
'fields':['R % RW'],
'comment':["The table's pecentage of read requests of the total RW requests (read and Write) in the cluster."]
},{
'fields':['W % RW'],
'comment':["The table's pecentage of write requests of the total RW requests (read and Write) in the cluster."]
},{
'fields':['Average TPS'],
'comment':["The table's read or write request count divided by the uptime."]
},{
'fields':['Read TPS'],
'comment':["The cluster's average read requests per second. The time is determined by the node's uptime."]
},{
'fields':['Read TPMo'],
'comment':["The cluster's average read requests per month. The month is calculated at 365.25/12 days."]
},{
'fields':['Write TPS'],
'comment':["The cluster's average write requests per second. The time is determined by the node's uptime."]
},{
'fields':['Write TPMo'],
'comment':["The cluster's average write requests per month. The month is calculated at 365.25/12 days."]
},{
'fields':['Uptime (sec)','Uptime (day)'],
'comment':["The combined uptime of all nodes in the cluster"]
},{
'fields':['Total R % RW'],
'comment':["The total read requests percentage of combined RW requests (read and write) in the cluster."]
},{
'fields':['Total W % RW'],
'comment':["The total write requests percentage of combined RW requests (read and write) in the cluster."]
},{
'fields':['Memtable Row Size'],
'comment':["Average size of each record (row) in the memtables across all nodes."]
},{
'fields':['Memtable # Rows'],
'comment':["The total number of records (rows) in the memtables across all nodes"]
},{
'fields':['Avg Row Size'],
'comment':["The average size of each records (row) in tablehistograms across all nodes."]
},{
'fields':['Sample # Rows'],
'comment':["The total number of records (rows) in tablehistograms across all nodes"]
}
]


# run through each cluster diag file path listed in command line
for cluster_url in data_url:

  # initialize cluster vaariables
  cluster_name=''
  is_index = 0
  read_subtotal = 0
  write_subtotal = 0
  total_size = 0
  dc_total_size = 0
  total_reads = 0
  total_writes = 0
  read_count = []
  write_count =[]
  table_count =[]
  table_tps={}
  table_row_size = {}
  total_rw = 0
  ks_array = []
  count = 0
  size_table = {}
  read_table = {}
  write_table = {}
  size_totals = {}
  table_totals = {}
  total_uptime = 0
  dc_array = []
  cluster_gcpause = []
  node_dc = {}
  dc_list = []
  dc_gcpause = {}
  node_gcpause = {}
  gc_data = {}
  gc_dt = []
  wname = 'gc_data'
  newest_gc = {}
  oldest_gc = {}
  max_gc = {}
  exclude_tab = []
  node_uptime = {}

  rootPath = cluster_url + '/nodes/'

  # collect dc info
  for node in os.listdir(rootPath):
    ckpath = rootPath + node + '/nodetool'
    if path.isdir(ckpath):
      statuspath = rootPath + node + '/nodetool/status'
      get_dc(statuspath)

    schemapath = rootPath + node + '/driver'
    if path.isdir(schemapath):
      try:
        schemaFile = open(schemapath + '/schema', 'r')
      except:
        exit('Error: No schema file - ' + schemapath + '/schema')

  # collect and analyze schema
  ks = ''
  for node in os.listdir(rootPath):
    if (ks==''):
      ckpath = rootPath + node + '/nodetool'
      if path.isdir(ckpath):
        ks = ''
        tbl = ''
        create_stmt = {}
        tbl_data = {}
        for line in schemaFile:
          line = line.strip('\n').strip()
          if("CREATE KEYSPACE" in line):
            prev_ks = ks
            ks = line.split()[2].strip('"')
            tbl_data[ks] = {'cql':line,'rf':0}
            rf=0;
            for dc_name in dc_array:
              if ("'"+dc_name+"':" in line):
                i=0
                for prt in line.split():
                  prt_chk = "'"+dc_name+"':"
                  if (prt==prt_chk):
                    rf=line.split()[i+1].strip('}').strip(',').strip("'")
                    tbl_data[ks]['rf']+=float(rf)
                  i+=1
              elif("'replication_factor':" in line):
                i=0
                for prt in line.split():
                  prt_chk = "'replication_factor':"
                  if (prt==prt_chk):
                    rf=line.split()[i+1].strip('}').strip(',').strip("'")
                    tbl_data[ks]['rf']+=float(rf)
                  i+=1
              else:tbl_data[ks]['rf']=float(1)
          elif('CREATE INDEX' in line):
            prev_tbl = tbl
            tbl = line.split()[2].strip('"')
            tbl_data[ks][tbl] = {'type':'Index', 'cql':line}
          elif('CREATE CUSTOM INDEX' in line):
            prev_tbl = tbl
            tbl = line.split()[2].strip('"')
            tbl_data[ks][tbl] = {'type':'Custom Index', 'cql':line}
          elif('CREATE TYPE' in line):
            prev_tbl = tbl
            tbl_line = line.split()[2].strip()
            tbl = tbl_line.split('.')[1].strip().strip('"')
            tbl_data[ks][tbl] = {'type':'Type', 'cql':line}
            tbl_data[ks][tbl]['field'] = {}
          elif('CREATE TABLE' in line):
            prev_tbl = tbl
            tbl_line = line.split()[2].strip()
            tbl = tbl_line.split('.')[1].strip().strip('"')
            tbl_data[ks][tbl] = {'type':'Table', 'cql':line}
            tbl_data[ks][tbl]['field'] = {}
          elif('CREATE MATERIALIZED VIEW' in line ):
            prev_tbl = tbl
            tbl_line = line.split()[3].strip()
            tbl = tbl_line.split('.')[1].strip().strip('"')
            tbl_data[ks][tbl] = {'type':'Materialized View', 'cql':line}
            tbl_data[ks][tbl]['field'] = {}
          elif('PRIMARY KEY' in line):
            if(line.count('(') == 1):
              tbl_data[ks][tbl]['pk'] = [line.split('(')[1].split(')')[0].split(', ')[0]]
              tbl_data[ks][tbl]['cc'] = line.split('(')[1].split(')')[0].split(', ')
              del tbl_data[ks][tbl]['cc'][0]
            elif(line.count('(') == 2):
              tbl_data[ks][tbl]['pk'] = line.split('(')[2].split(')')[0].split(', ')
              tbl_data[ks][tbl]['cc'] = line.split('(')[2].split(')')[1].lstrip(', ').split(', ')
            tbl_data[ks][tbl]['cql'] += ' ' + line.strip()
          elif line != '' and line.strip() != ');':
            try:
              tbl_data[ks][tbl]['cql'] += ' ' + line
              if('AND ' not in line and ' WITH ' not in line):
                fld_name = line.split()[0]
                fld_type = line.split()[1].strip(',')
                tbl_data[ks][tbl]['field'][fld_name]=fld_type
            except:
              print('Error1:' + ks + '.' + tbl + ' - ' + line)

  # begin looping through each node and collect node info
  tbl_row_size = {}
  for node in os.listdir(rootPath):
    ckpath = rootPath + node + '/nodetool'
    if path.isdir(ckpath):
      
      # initialize node variables
      iodata = {}
      iodata[node] = {}
      keyspace = ''
      table = ''
      dc = ''
      cfstat = rootPath + node + '/nodetool/cfstats'
      tablestat = rootPath + node + '/nodetool/tablestats'
      clusterpath = rootPath + node + '/nodetool/describecluster'
      infopath = rootPath + node + '/nodetool/info'

      #collect cluster name
      if (cluster_name == ''):
        cluster_name = get_param(clusterpath,'Name:',1)

      # collect and analyze uptime and R/W counts from cfstats
      try:
        cfstatFile = open(cfstat, 'r')
      except:
        cfstatFile = open(tablestat, 'r')
      node_uptime[node] = int(get_param(infopath,'Uptime',3))
      total_uptime = total_uptime + node_uptime[node]

      ks = ''
      tbl = ''
      ks_type=''
      for line in cfstatFile:
        line = line.strip('\n').strip()
        if (line==''): tbl = ''
        else:
          if('Keyspace' in line):
            ks = line.split(':')[1].strip()
          if (ks<>''):
            try:
              type(table_tps[ks])
            except:
              table_tps[ks]={}
            if('Table: ' in line):
              tbl = line.split(':')[1].strip()
              is_index = 0
            elif('Table (index): ' in line):
              tbl = line.split(':')[1].strip()
              is_index = 1
            if(tbl<>''):
              try:
                type(table_tps[ks][tbl])
              except:
                table_tps[ks][tbl]={'write':0,'read':0}
              if ('Space used (total):' in line):
                tsize = float(line.split(':')[1].strip())
                if (tsize):
                  total_size += tsize
                  # astra pricing will be based on data on one set of data
                  # divide the total size by the total rf (gives the size per node)
                  try:
                    type(tbl_data[ks])
                  except:
                    tbl_data[ks] = {}
                    tbl_data[ks]['rf'] = float(1)
                  try:
                    type(size_table[ks])
                  except:
                    size_table[ks] = {}
                  try:
                    type(size_table[ks][tbl])
                    size_table[ks][tbl] += tsize
                  except:
                    size_table[ks][tbl] = tsize
              elif ('Memtable data size:' in line):
                tsize = float(line.split(':')[1].strip())
                if (tsize):
                  total_size += tsize
                  # astra pricing will be based on data on one set of data
                  # divide the total size by the total rf (gives the size per node)
                  try:
                    type(tbl_data[ks])
                  except:
                    tbl_data[ks] = {}
                    tbl_data[ks]['rf'] = float(1)
                  try:
                    type(size_table[ks])
                  except:
                    size_table[ks] = {}
                  try:
                    type(size_table[ks][tbl])
                    size_table[ks][tbl] += tsize
                  except:
                    size_table[ks][tbl] = tsize
              elif('Local read count: ' in line):
                count = int(line.split(':')[1].strip())
                if (count > 0):
                  total_reads += count
                  table_tps[ks][tbl]['read'] += float(count) / float(node_uptime[node])
                  try:
                    type(read_table[ks])
                  except:
                    read_table[ks] = {}
                  try:
                    type(read_table[ks][tbl])
                    read_table[ks][tbl] += count
                  except:
                    read_table[ks][tbl] = count
              if (is_index == 0):
                if('Local write count: ' in line):
                  count = int(line.split(':')[1].strip())
                  if (count > 0):
                    table_tps[ks][tbl]['write'] += float(count) / float(node_uptime[node])
                    try:
                      total_writes += count
                    except:
                      total_writes += count
                    try:
                      type(write_table[ks])
                    except:
                      write_table[ks] = {}
                    try:
                      type(write_table[ks][tbl])
                      write_table[ks][tbl] += count
                    except:
                      write_table[ks][tbl] = count

  # total up R/W across all nodes
  for ks,readtable in read_table.items():
    for tablename,tablecount in readtable.items():
      read_count.append({'keyspace':ks,'table':tablename,'count':tablecount})
  for ks,writetable in write_table.items():
    for tablename,tablecount in writetable.items():
      try:
        write_count.append({'keyspace':ks,'table':tablename,'count':tablecount})
      except:
        write_count.append({'keyspace':ks,'table':tablename,'count':tablecount})

  # total up data size across all nodes
  for ks,sizetable in size_table.items():
    for tablename,tablesize in sizetable.items():
      table_count.append({'keyspace':ks,'table':tablename,'count':tablesize})

  # sort R/W data
  read_count.sort(reverse=True,key=sortFunc)
  write_count.sort(reverse=True,key=sortFunc)
  table_count.sort(reverse=True,key=sortFunc)
  total_rw = total_reads+total_writes
  

  #initialize GC variables
  cluster_gcpause = []
  node_dc = {}
  dc_list = []
  dc_gcpause = {}
  node_gcpause = {}
  gc_data = {}
  gc_dt = []
  wname = 'gc_data'
  newest_gc = {}
  oldest_gc = {}
  max_gc = {}
  
  # collect GC Data
  rootPath = cluster_url + '/nodes/'
  for node in os.listdir(rootPath):
    systemlogpath = rootPath + node + '/logs/cassandra/'
    systemlog = systemlogpath + 'system.log'
    jsppath1 = rootPath + node + '/java_system_properties.json'
    jsppath2 = rootPath + node + '/java_system_properties.txt'
    infopath = rootPath + node + '/nodetool/info'
    if(path.exists(systemlog)):
      statuspath = rootPath + node + '/nodetool/status'
      if(len(node_dc)==0):
        get_dc(statuspath)
        newest_gc[cluster_name]={'jd':0.0,'dt':''}
        oldest_gc[cluster_name]={'jd':99999999999.9,'dt':''}
        max_gc[cluster_name]=''
      node_gcpause[node] = []
      newest_gc[node]={'jd':0.0,'dt':''}
      oldest_gc[node]={'jd':99999999999.9,'dt':''}
      max_gc[node]=''
      tz[node]='UTC'
      
      for logfile in os.listdir(systemlogpath):
        if(logfile.split('.')[0] == 'system'):
          systemlog = systemlogpath + logfile
          cor_node = node.replace('-','.')
          parseGC(cor_node,systemlog,systemlogpath)


  # collect GC data from additional log path
  addlogs = './AdditionalLogs'
  if(path.exists(addlogs)):
    for node in os.listdir(addlogs):
      dirpath = 'AdditionalLogs/' + node
      if(node.split('-')[0]=='10'):
        logdir = 'AdditionalLogs/' + node + '/var/log/cassandra'
        for logfile in os.listdir(logdir):
          if(logfile.split('.')[0] == 'system'):
            systemlogpath = logdir + '/'
            systemlog = systemlogpath + '/' + logfile
            cor_node = node.replace('-','.')
            parseGC(cor_node,systemlog,systemlogpath)

  #collect cluster GC Percents
  get_gc_data('Cluster',cluster_name,cluster_gcpause,0)

  for dc, dc_pause in dc_gcpause.items():
    get_gc_data('DC',dc,dc_pause,0)
  for node, node_pause in node_gcpause.items():
    get_gc_data('Node',node,node_pause,1)

  # Create DC List
  for node_val, dc_val in node_dc.items():
    dc_list.append(dc_val)
  dc_list = list(dict.fromkeys(dc_list))
  dc_list.sort()

  # Create Workbook
  stats_sheets = {}
  worksheet = {}
  workbook = xlsxwriter.Workbook(cluster_url + '/' + cluster_name + '_' + 'astra_chart' + '.xlsx')
  
  # Create Tabs
  worksheet_chart = workbook.add_worksheet('Astra Chart')
  worksheet = workbook.add_worksheet('Workload')
  worksheet.freeze_panes(3,0)
  ds_worksheet = workbook.add_worksheet('Data Size')
  ds_worksheet.freeze_panes(2,2)
  gc_worksheet = workbook.add_worksheet('GC Pauses')
  gc_worksheet.freeze_panes(2,2)
  for sheet_array in sheets_data:
    if (sheet_array['sheet_name'] not in exclude_tab):
      stats_sheets[sheet_array['sheet_name']] = workbook.add_worksheet(sheet_array['tab_name'])
      stats_sheets[sheet_array['sheet_name']].freeze_panes(sheet_array['freeze_row'],sheet_array['freeze_col'])

  # Create Formats
  header_format1 = workbook.add_format({
      'bold': True,
      'italic' : True,
      'text_wrap': False,
      'font_size': 14,
      'border': 1,
      'valign': 'top'})

  header_format2 = workbook.add_format({
      'bold': True,
      'text_wrap': False,
      'font_size': 12,
      'border': 1,
      'valign': 'top'})

  header_format3 = workbook.add_format({
      'bold': True,
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'valign': 'top'})
      
  header_format4 = workbook.add_format({
      'bold': True,
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'font_color': 'white',
      'bg_color': '#3980D3',
      'valign': 'top'})
      
  data_format = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'valign': 'top'})

  data_format2 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'italic': True,
      'valign': 'top'})
      
  perc_format = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '#,###.00%',
      'valign': 'top'})

  num_format1 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '#,###',
      'valign': 'top'})

  num_format2 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '#,##0.00',
      'valign': 'top'})

  total_format1 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '[>999999999]0.000,,," GB";[>999999]0.000,," MB";0.000," KB"',
      'valign': 'top'})

  tps_format1 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '[>999999999]0.000,,," GB/Sec";[>999999]0.000,," MB/Sec";0.000," KB/Sec"',
      'valign': 'top'})

  tpmo_format1 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '[>999999999]0.000,,," GB/Mo";[>999999]0.000,," MB/Mo";0.000," KB/Mo"',
      'valign': 'top'})

  num_format3 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'font_color': 'white',
      'bg_color': '#3980D3',
      'num_format': '#,###',
      'valign': 'top'})

  title_format = workbook.add_format({
      'bold': 1,
      'font_size': 13,
      'border': 1,
      'align': 'center',
      'valign': 'vcenter',
      'font_color': 'white',
      'bg_color': '#EB6C34'})

  title_format2 = workbook.add_format({
      'bold': 1,
      'font_size': 13,
      'border': 1,
      'align': 'center',
      'valign': 'vcenter',
      'font_color': 'white',
      'bg_color': '#998E5D'})
      
  title_format3 = workbook.add_format({
      'bold': 1,
      'font_size': 14,
      'border': 1,
      'align': 'center',
      'valign': 'vcenter',
      'font_color': 'white',
      'bg_color': '#3A3A42'})

  title_format4 = workbook.add_format({
      'bold': 1,
      'font_size': 13,
      'border': 1,
      'align': 'left',
      'valign': 'vcenter',
      'font_color': 'white',
      'bg_color': '#EB6C34'})

  ds_worksheet.merge_range('A1:E1', 'Table Size', title_format)

  ds_headers=['Keyspace','Table','Table Size','RF','Data Set Size']
  ds_headers_width=[14,25,17,4,17]

  column=0
  for col_width in ds_headers_width:
    ds_worksheet.set_column(column,column,col_width)
    column+=1


  column=0
  for header in ds_headers:
      if header == '':
        ds_worksheet.write(1,column,header)
      else:
        ds_worksheet.write(1,column,header,header_format1)
        write_cmt(ds_worksheet,chr(ord('@')+column+1)+'2',header)
      column+=1

  row = 2
  perc_reads = 0.0
  column = 0
  total_t_size = 0
  total_set_size = 0.0
  total_row = {'read':0,'write':0,'size':0}

  cluster_name = ''
  prev_nodes = []
  stat_sheets = {}
  headers = {}
  col_widths = {}
  sheets_record = {}
  row = {}
  node_status = 1
  proxyhistData = {}

  for node in os.listdir(rootPath):
    ckpath = rootPath + node + '/nodetool'
    if(path.isdir(ckpath)):
      if cluster_name == '':
        clusterpath = rootPath + node + '/nodetool/describecluster'
        cluster_name = get_param(clusterpath,'Name:',1)

        for sheet_array in sheets_data:
          if (sheet_array['sheet_name'] not in exclude_tab):
            headers[sheet_array['sheet_name']] = sheet_array['headers']
            col_widths[sheet_array['sheet_name']] = sheet_array['widths']
            sheets_record[sheet_array['sheet_name']]={}

        for sheet_name,sheet_obj in stats_sheets.items():
          if (sheet_name == 'ph'):
            sheet_obj.merge_range('A1:F1','Coordinating Node Read Latency',title_format3)
            sheet_obj.merge_range('H1:M1','Coordinating Node Write Latency',title_format3)
            row[sheet_name]=1
          else:
            row[sheet_name]=0
          for col_num,header in enumerate(headers[sheet_name]):
            if header <> '':
              sheet_obj.write(row[sheet_name],col_num,header,title_format)
          for col_num,col_width in enumerate(col_widths[sheet_name]):
            sheet_obj.set_column(col_num,col_num,col_width)
          row[sheet_name]+=1

      # collect dc name
      dc = ''
      info = rootPath + node + '/nodetool/info'
      infoFile = open(info, 'r')
      for line in infoFile:
        if('Data Center' in line):
          dc = line.split(':')[1].strip()

      # collect node data
      if(node_status):
        status = rootPath + node + '/nodetool/status'
        statusFile = open(status, 'r')
        
        for line in statusFile:
          if('Datacenter:' in line):
            datacenter = line.split(':')[1].strip()
          elif(line.count('.')>=3):
            values = line.split();
            row_data = [values[1],datacenter,values[2] + ' ' + values[3],values[4],values[7]]
            write_row('node',row_data,data_format)
            node_status=0

      # collect data from the cfstats log file
      keyspace = ''
      table = ''
      cfstat = rootPath + node + '/nodetool/cfstats'
      cfstatFile = open(cfstat, 'r')
      for line in cfstatFile:
        if('Keyspace' in line):
          keyspace = line.split(':')[1].strip()
        elif('Table: ' in line and keyspace not in system_keyspace):
          table = line.split(':')[1].strip()
        elif(':' in line and keyspace not in system_keyspace):
          header = line.split(':')[0].strip()
          value = line.split(':')[1].strip()
          row_data = [node,dc,keyspace,table,header,value]
 
          for sheet_array in sheets_data:
            if (sheet_array['sheet_name'] not in exclude_tab):
              if(sheet_array['cfstat_filter'] and sheet_array['cfstat_filter'] in line):
                value = line.split(':')[1].strip()
                row_data = [node,dc,keyspace,table,value]
                if (sheet_array['filter_type']):
                  value = value.strip(sheet_array['strip'])
                  if (sheet_array['filter_type']=='>=' and float(value)>=float(sheet_array['filter'])):
                    if(sheet_array['sheet_name']=='partition'):
                      row_data[4] = str(int(value)/1000000)
                    if(sheet_array['extra']):
                      sheets_record[sheet_array['sheet_name']][row[sheet_array['sheet_name']]] = row_data
                      row[sheet_array['sheet_name']]+=1
                    else:
                      write_row(sheet_array['sheet_name'],row_data,data_format)
                else:
                  write_row(sheet_array['sheet_name'],row_data,data_format)

      # organize key data
      key_record = {}
      key_data = {}
      for sheet_array in sheets_data:
        if (sheet_array['sheet_name'] not in exclude_tab):
          if(sheet_array['extra']):
            row[sheet_array['sheet_name']]=1
            for record_num,record in sheets_record[sheet_array['sheet_name']].items():
              new_key = sheet_array['sheet_name']+'_'+record[2]+'_'+record[3]
              if hasattr(key_record,new_key) :
                if(key_record[new_key] < record[4]):
                  key_record[new_key] = record[4]
                  key_data[new_key] = record
              else:
                key_record[new_key] = record[4]
                key_data[new_key] = record

      # collect node R/W latency data - coordinator level latencies
      proxyhistData[node] = []
      proxyhist = rootPath + node + '/nodetool/proxyhistograms'
      proxyhistFile = open(proxyhist, 'r')
      proxyhistData[node] = {'99%':[],'98%':[],'95%':[],'75%':[],'50%':[]}
      for line in proxyhistFile:
        if('Datacenter:' in line):
          datacenter = line.split(':')[1].strip()
        elif('Percentile' in line and header==''):
          test = line.split();
        elif('%' in line):
          values = line.split();
          readlat = float(values[1])/1000
          writelat = float(values[2])/1000
          row_data = [node,dc,values[0],readlat,writelat]
          proxyhistData[node][values[0]].append([readlat,writelat])
                  
      for row_key in key_record:
        write_row(row_key.split('_')[0],key_data[row_key],data_format)
      
      for nodeid,ph_datarow in proxyhistData.items():
        if nodeid not in prev_nodes:
          row_data = [nodeid,round(ph_datarow['99%'][0][0],2),round(ph_datarow['98%'][0][0],2),round(ph_datarow['95%'][0][0],2),round(ph_datarow['75%'][0][0],2),round(ph_datarow['50%'][0][0],2),'',nodeid,round(ph_datarow['99%'][0][1],2),round(ph_datarow['98%'][0][1],2),round(ph_datarow['95%'][0][1],2),round(ph_datarow['75%'][0][1],2),round(ph_datarow['50%'][0][1],2)]
          write_row('ph',row_data,data_format,[6])
          prev_nodes.append(nodeid)

  # create workload tab
  wl_headers=['Keyspace','Table','Read Requests','Average TPS','% Reads','R % RW','','Keyspace','Table','Write Requests','Average TPS','% Writes','W % RW']
  wl_headers_width=[14,25,17,13,9,9,3,14,25,17,13,9,9]

  column=0
  for col_width in wl_headers_width:
    worksheet.set_column(column,column,col_width)
    column+=1

  worksheet.merge_range('A1:M1', 'Workload for '+cluster_name, title_format3)
  worksheet.merge_range('A2:F2', 'Reads', title_format)
  worksheet.merge_range('H2:M2', 'Writes', title_format)

  column=0
  for header in wl_headers:
      if header == '':
        worksheet.write(2,column,header)
      else:
        worksheet.write(2,column,header,header_format1)
        write_cmt(worksheet,chr(ord('@')+column+1)+'3',header)
      column+=1

  row = 2
  column = 0
 
  for t_data in table_count:
    ks = t_data['keyspace']
    tbl = t_data['table']
    cnt = t_data['count']
    ds_worksheet.write(row,column,ks,data_format)
    ds_worksheet.write(row,column+1,tbl,data_format)
    ds_worksheet.write(row,column+2,cnt,total_format1)
    ds_worksheet.write(row,column+3,tbl_data[ks]['rf'],num_format1)
    ds_worksheet.write(chr(ord('@')+column+5)+str(row+1),'='+chr(ord('@')+column+3)+str(row+1)+'/'+chr(ord('@')+column+4)+str(row+1),total_format1)

    row+=1

  total_row['size']=row

  ds_worksheet.write(row,column,'Total',header_format4)
  ds_worksheet.write(row,column+2,'=SUM(C3:E'+ str(row)+')',total_format1)
  ds_worksheet.write(row,column+4,'=SUM(E3:E'+ str(row)+')',total_format1)

  row = 3
  perc_reads = 0.0
  column = 0

  for reads in read_count:
    perc_reads = float(read_subtotal) / float(total_reads)
    ks = reads['keyspace']
    tbl = reads['table']
    cnt = reads['count']
    try:
      type(table_totals[ks])
    except:
      table_totals[ks] = {}
    try:
      if type(table_totals[ks])>1:
        div_by=2
      else:
        div_by=1
    except:
      div_by=1
    table_totals[ks][tbl] = {'reads':cnt,'writes':'n/a'}
    read_subtotal += cnt
    worksheet.write(row,column,ks,data_format)
    worksheet.write(row,column+1,tbl,data_format)
    worksheet.write(row,column+2,cnt/div_by,num_format1)
    worksheet.write(row,column+3,table_tps[ks][tbl]['read']/div_by,num_format2)
    worksheet.write(row,column+4,float(cnt)/total_reads,perc_format)
    worksheet.write(row,column+5,float(cnt)/float(total_rw),perc_format)
    row+=1

  total_row['read']=row
  
  worksheet.write(row,column,'Total',header_format4)
  write_cmt(worksheet,chr(ord('@')+column+1)+str(row+1),'Total')
  worksheet.write(row,column+2,'=SUM(C4:C'+ str(row)+')',total_format1)
  worksheet.write(row,column+3,'=SUM(D4:D'+ str(row)+')',tps_format1)
  worksheet.write(row,column+5,'=SUM(F4:F'+ str(row)+')',perc_format)
  write_cmt(worksheet,chr(ord('@')+column+6)+str(row+1),'Total R % RW')

  perc_writes = 0.0
  row = 3
  column = 7
  astra_write_subtotal = 0
  for writes in write_count:
    ks = writes['keyspace']
    tbl = writes['table']
    cnt = writes['count']
    try:
      type(table_totals[ks])
    except:
      table_totals[ks] = {}
    try:
      type(table_totals[ks][tbl])
      table_totals[ks][tbl] = {'reads':table_totals[ks][tbl]['reads'],'writes':cnt}
    except:
      table_totals[ks][tbl] = {'reads':'n/a','writes':cnt}
    worksheet.write(row,column,ks,data_format)
    worksheet.write(row,column+1,tbl,data_format)
    worksheet.write(row,column+2,cnt/len(tbl_data[ks][tbl]['field']),num_format1)
    worksheet.write(row,column+3,table_tps[ks][tbl]['write']/tbl_data[ks]['rf'],num_format2)
    worksheet.write(row,column+4,float(cnt)/total_writes,perc_format)
    worksheet.write(row,column+5,float(cnt)/float(total_rw),perc_format)
    row+=1

  total_row['write']=row

  worksheet.write(row,column,'Total',header_format4)
  write_cmt(worksheet,chr(ord('@')+column+1)+str(row+1),'Total')
  worksheet.write(row,column+2,'=SUM(J4:J'+ str(row)+')',total_format1)
  worksheet.write(row,column+3,'=SUM(K4:K'+ str(row)+')',tps_format1)
  worksheet.write(row,column+5,'=SUM(M4:M'+ str(row)+')',perc_format)
  write_cmt(worksheet,chr(ord('@')+column+6)+str(row+1),'Total W % RW')

  # create the Astra Chart tab
  row=1
  column=0
  worksheet_chart.merge_range('A1:B1', 'Astra Conversion Info for '+cluster_name, title_format3)
  worksheet_chart.set_column(0,0,25)
  worksheet_chart.set_column(1,1,14)
  worksheet_chart.write(row,column,'Read TPS',title_format4)
  write_cmt(worksheet_chart,chr(ord('@')+column+1)+str(row+1),'Read TPS')
  worksheet_chart.write_formula('B2','=Workload!D'+str(total_row['read']+1),tps_format1)
  worksheet_chart.write(row+1,column,'Read TPMo',title_format4)
  write_cmt(worksheet_chart,chr(ord('@')+column+1)+str(row+2),'Read TPMo')
  worksheet_chart.write_formula('B3','=Workload!D'+str(total_row['read']+1)+'*60*60*24*365.25/12',tps_format1)
  worksheet_chart.write(row+2,column,'Write TPS',title_format4)
  write_cmt(worksheet_chart,chr(ord('@')+column+1)+str(row+3),'Write TPS')
  worksheet_chart.write_formula('B4','=Workload!K'+str(total_row['write']+1),tps_format1)
  worksheet_chart.write(row+3,column,'Write TPMo',title_format4)
  write_cmt(worksheet_chart,chr(ord('@')+column+1)+str(row+4),'Write TPMo')
  worksheet_chart.write_formula('B5','=Workload!K'+str(total_row['write']+1)+'*60*60*24*365.25/12',tps_format1)
  worksheet_chart.write(row+4,column,'Data Size (GB)',title_format4)
  write_cmt(worksheet_chart,chr(ord('@')+column+1)+str(row+5),'Data Size')
  worksheet_chart.write_formula('B6',"='Data Size'!E"+str(total_row['size']+1),total_format1)

  gc_headers=['Name','Level/DC','Pauses','Max','P99','P98','P95','P90','P75','P50','Min','From','To','Max Date']

  gc_fields=['Name','Level','Pauses','Max','P99','P98','P95','P90','P75','P50','Min','From','To','max_gc']
  gc_widths=[18,14,8,6,6,6,6,6,6,6,6,35,35,17]

  prev_dc=0
  row=0
  column=0
  for header in gc_headers:
    gc_worksheet.write(row,column,header,title_format)
    write_cmt(worksheet,chr(ord('@')+column+1)+str(row+1),header)
    column+=1

  for col_num,col_width in enumerate(gc_widths):
    gc_worksheet.set_column(col_num,col_num,col_width)

  column=0
  for name, gc_val in gc_data.items():
    if(gc_val['Level']=='Cluster'):
      row+=1
      for field in gc_fields:
        if(field=='From'):
          gc_worksheet.write(row,column,oldest_gc[name]['dt'],data_format)
        elif(field=='To'):
          gc_worksheet.write(row,column,newest_gc[name]['dt'])
        elif(field=='max_gc'):
          gc_worksheet.write(row,column,max_gc[name])
        else:
          gc_worksheet.write(row,column,gc_val[field])
        column+=1
      row+=1
      column=0

  dc_count=0
  for name, gc_val in gc_data.items():
    if(gc_val['Level']=='DC'):
      dc_count += 1
      for field in gc_fields:
        if(field=='From'):
          gc_worksheet.write(row,column,oldest_gc[name]['dt'])
        elif(field=='To'):
          gc_worksheet.write(row,column,newest_gc[name]['dt'])
        elif(field=='max_gc'):
          gc_worksheet.write(row,column,max_gc[name])
        elif(gc_val[field]):
          gc_worksheet.write(row,column,gc_val[field])
        column+=1

      row+=1
      column=0

  for dc_name in dc_list:
    for name, gc_val in gc_data.items():
      node_ip = gc_val['Name']
      if(gc_val['Level']=='Node' and dc_name==node_dc[node_ip]):
        for field in gc_fields:
          if(field=='Level'):
            gc_worksheet.write(row,column,node_dc[gc_val['Name']])
          elif(field=='From'):
            gc_worksheet.write(row,column,oldest_gc[name]['dt'])
          elif(field=='To'):
            gc_worksheet.write(row,column,newest_gc[name]['dt'])
          elif(field=='max_gc'):
            gc_worksheet.write(row,column,max_gc[name])
          elif(gc_val[field]):
            gc_worksheet.write(row,column,gc_val[field])
          column+=1
        row+=1
        column=0

  worksheet_chart.activate()
  workbook.close()
  print('"' + cluster_name + '_' + 'astra_chart' + '.xlsx"' + ' was created in "' + cluster_url) +'"'
exit();

