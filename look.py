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
    try:
      type(dc_array[dc])
    except:
      dc_array.append(dc)
  else:
    exit('ERROR: No File: ' + filepath)


# Organize primary support tab information
sheets_data = []
sheets_data.append({'sheet_name':'node','tab_name':'Node Data','cfstat_filter':'','headers':['Node','DC','Load','Tokens','Rack'],'widths':[18,14,14,8,11],'extra':0})
sheets_data.append({'sheet_name':'ph','tab_name':'Proxihistogram','cfstat_filter':'','headers':['Node','P99','P98','95%','P75','P50','','Node','P99','P98','95%','P75','P50'],'widths':[18,5,5,5,5,5,3,18,5,5,5,5,5],'extra':0})
sheets_data.append({'sheet_name':'dmutation','tab_name':'Dropped Mutation','cfstat_filter':'Dropped Mutations','headers':['Node','DC','Keyspace','Table','Dropped Mutations'],'widths':[18,14,14,25,20],'filter_type':'>=','filter':1,'strip':'','extra':0})
sheets_data.append({'sheet_name':'numTables','tab_name':'Table Qty','cfstat_filter':'Total number of tables','headers':['Node','DC','Keyspace','Table','Total Number of Tables'],'widths':[18,14,14,25,23],'filter_type':'>=','filter':100,'strip':'','extra':0})
sheets_data.append({'sheet_name':'partition','tab_name':'Wide Partitions','cfstat_filter':'Compacted partition maximum bytes','headers':['Example Node','DC','Keyspace','Table','Partition Size(MB)'],'widths':[18,14,14,25,18],'filter_type':'>=','filter':100000000,'strip':'','extra':1})
sheets_data.append({'sheet_name':'sstable','tab_name':'SSTable Count','cfstat_filter':'SSTable count','headers':['Example Node','DC','Keyspace','Table','SSTable Count'],'widths':[18,14,14,25,15],'filter_type':'>=','filter':15,'strip':'','extra':1})
sheets_data.append({'sheet_name':'rlatency','tab_name':'Read Latency','cfstat_filter':'Local read latency','headers':['Node','DC','Keyspace','Table','Read Latency (ms)'],'widths':[18,14,14,25,20],'filter_type':'>=','filter':5,'strip':'ms','extra':0})
sheets_data.append({'sheet_name':'wlatency','tab_name':'Write Latency','cfstat_filter':'Local write latency','headers':['Node','DC','Keyspace','Table','Write Latency (ms)'],'widths':[18,14,14,25,20],'filter_type':'>=','filter':1,'strip':'ms','extra':0})
#sheets_data.append({'sheet_name':'ts','tab_name':'Tombstones','headers':['Node','DC','Keyspace','Table','Write Latency (ms)'],'extra':0})

system_keyspace = ['OpsCenter','dse_insights_local','solr_admin','test','dse_system','dse_analytics','system_auth','system_traces','system','dse_system_local','system_distributed','system_schema','dse_perf','dse_insights','dse_security','dse_system','killrvideo','dse_leases','dsefs_c4z','HiveMetaStore','dse_analytics','dsefs','spark_system']
ks_type_abbr = {'app':'Application','sys':'System'}


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

# collect and analyze command line arguments
for argnum,arg in enumerate(sys.argv):
  if(arg=='-h' or arg =='--help'):
    show_help = 'y'
  elif(arg=='-p'):
    data_url.append(sys.argv[argnum+1])

# communicate command line help
if show_help:
  help_content = \
  'usage: look.py [-h] [--help] [-inc_yaml]\n'\
  '                       [-p PATH_TO_DIAG_FOLDER]\n'\
  '                       [-rt READ_THRESHOLD]\n'\
  '                       [-wt WRITE_THRESHOLD]\n'\
  '                       [-sys INCLUDE SYSTEM KEYSPACES]\n'\
  'optional arguments:\n'\
  '-h, --help             This help info\n'\
  '-p                     Path to the diagnostics folder\n'\
  '                        Multiple diag folders accepted\n'\
  '                        i.e. -p PATH1 -p PATH2 -p PATH3\n'\

  exit(help_content)

# run through each cluster diag file path listed in command line
for cluster_url in data_url:

  # initialize cluster vaariables
  cluster_name=''
  is_index = 0
  read_subtotal = 0
  write_subtotal = 0
  total_size = 0
  astra_size = 0
  dc_total_size = 0
  total_reads = 0
  total_writes = 0
  astra_total_writes = 0
  read_count = []
  write_count =[]
  astra_write_count =[]
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
  heap_size = {}
  heap_used = {}
  off_heap = {}
  newsize = {}
  maxgcpause = {}
  ihop = {}
  maxten = {}
  par_threads = {}
  conc_threads = {}
  reg_size = {}
  max_dir_mem = {}
  thread_stack_size = {}
  gc_data = {}
  gc_dt = []
  wname = 'gc_data'
  newest_gc = {}
  oldest_gc = {}
  max_gc = {}
  exclude_tab = []

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
    else:
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
          if('CREATE KEYSPACE' in line):
            prev_ks = ks
            ks_array.append(ks)
            ks = line.split()[2].strip('"')
            tbl_data[ks] = {'cql':line,'rf':0}
            rf=0;
            for dc_name in dc_array:
              i=0
              for prt in line.split():
                prt_chk = "'"+dc_name+"':"
                if (prt==prt_chk):
                  rf=line.split()[i+1].strip('}').strip(',').strip("'")
                  tbl_data[ks]['rf']+=float(rf)
                i+=1
            if (rf==0.0):
              tbl_data[ks]['rf']=float(1)
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
      cfhistpath = rootPath + node + '/nodetool/cfhistograms'
      tblhistpath = rootPath + node + '/nodetool/tablehistograms'
      cfstat = rootPath + node + '/nodetool/cfstats'
      tablestat = rootPath + node + '/nodetool/tablestats'
      clusterpath = rootPath + node + '/nodetool/describecluster'
      infopath = rootPath + node + '/nodetool/info'

      #collect cluster name
      if (cluster_name == ''):
        cluster_name = get_param(clusterpath,'Name:',1)

      if(path.isfile(cfhistpath)):
        cfhistFile = open(cfhistpath, 'r')
        tblhist = 1
      elif(path.isfile(tblhistpath)):
        cfhistFile = open(tblhistpath, 'r')
        tblhist = 1
      else:
        tblhist = 0

      # collect row data sizes from cfhistograms
      if (tblhist==1):
        ks = ''
        tbl = ''
        is_tbl_data = 0
        for line in cfhistFile:
          line = line.strip('\n').strip()
          if (line==''):
            tbl = ''
            is_tbl_data = 0
          elif ('No SSTables exists' in line):
            is_tbl_data = 0
          if('/' in line):
            ks = line.split('/')[0].strip()
            if (ks not in system_keyspace):
              try:
                type(tbl_row_size[ks])
              except:
                tbl_row_size[ks] = {}
              tbl = line.split()[0].split('/')[1].strip()
              is_tbl_data = 1
              try:
                type(tbl_row_size[ks][tbl])
              except:
                tbl_row_size[ks][tbl] = 0
          if ('%' in line and is_tbl_data == 1 and ks not in system_keyspace):
            per = float(line.split()[0].strip().strip('%'))
            part_size = float(line.split()[4])
            cell_count = float(line.split()[5])
            num_fields = float(len(tbl_data[ks][tbl]['field']))
            tbl_row_size[ks][tbl] += (100-per)/100 * (part_size/(cell_count/num_fields)) / tbl_data[ks]['rf']
            
      # collect and analyze uptime and R/W counts from cfstats
      try:
        cfstatFile = open(cfstat, 'r')
      except:
        cfstatFile = open(tablestat, 'r')
      
      total_uptime = total_uptime + int(get_param(infopath,'Uptime',3))

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
            if('Table: ' in line):
              tbl = line.split(':')[1].strip()
              is_index = 0
            elif('Table (index): ' in line):
              tbl = line.split(':')[1].strip()
              is_index = 1
            if(tbl<>''):
              if ('Space used (total):' in line):
                tsize = float(line.split(':')[1].strip())
                if (tsize):
                  total_size += tsize
                  # astra pricing will be based on data on one set of data
                  # divide the total size by the total rf (gives the size per node)
                  try:
                    astra_size += tsize / tbl_data[ks]['rf']
                  except:
                    tbl_data[ks] = {}
                    tbl_data[ks]['rf'] = float(1)
                    astra_size += tsize / tbl_data[ks]['rf']
                  try:
                    type(size_table[ks])
                  except:
                    size_table[ks] = {}
                  try:
                    type(size_table[ks][tbl])
                    size_table[ks][tbl] += tsize / tbl_data[ks]['rf']
                  except:
                    size_table[ks][tbl] = tsize / tbl_data[ks]['rf']
              if('Local read count: ' in line):
                count = int(line.split(':')[1].strip())
                if (count > 0):
                  total_reads += count
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
                    try:
                      astra_total_writes += count / tbl_data[ks]['rf']
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
        astracount = tablecount / tbl_data[ks]['rf']
        astra_write_count.append({'keyspace':ks,'table':tablename,'count':astracount})
        write_count.append({'keyspace':ks,'table':tablename,'count':tablecount})
      except:
        astra_write_count.append({'keyspace':ks,'table':tablename,'count':tablecount})
        write_count.append({'keyspace':ks,'table':tablename,'count':tablecount})

  # sort R/W data
  read_count.sort(reverse=True,key=sortFunc)
  write_count.sort(reverse=True,key=sortFunc)
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
      if (path.isfile(jsppath1)):
        tz[node] = get_param(jsppath1,'user.timezone',2).strip(',').strip('"')
      elif (path.isfile(jsppath2)):
        tz[node] = get_param(jsppath2,'user.timezone',0).strip('user.timezone=')
      if (tz[node]=='Default'): tz[node] = 'UTC'
      
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
  ds_worksheet = workbook.add_worksheet('Data Size')
  gc_worksheet = workbook.add_worksheet('GC Pauses')
  for sheet_array in sheets_data:
    if (sheet_array['sheet_name'] not in exclude_tab):
      stats_sheets[sheet_array['sheet_name']] = workbook.add_worksheet(sheet_array['tab_name'])


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

  
  ds_worksheet.merge_range('A1:G1', 'Table Size', title_format)

  ds_headers=['Keyspace','Table','Table Size','RF','Data Set Size','Average Record Size','Est. # Records']
  ds_headers_width=[14,25,17,4,17,20,14]

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
      column+=1

  row = 2
  perc_reads = 0.0
  column = 0
  total_t_size = 0
  total_set_size = 0.0
  for ks,t_data in size_table.items():
    for tbl,t_size in t_data.items():
      total_t_size += t_size
      total_set_size += float(t_size)/float(tbl_data[ks]['rf'])
      ds_worksheet.write(row,column,ks,data_format)
      ds_worksheet.write(row,column+1,tbl,data_format)
      ds_worksheet.write(row,column+2,t_size,num_format1)
      ds_worksheet.write(row,column+3,tbl_data[ks]['rf'],num_format1)
      ds_worksheet.write(row,column+4,float(t_size)/float(tbl_data[ks]['rf']),num_format1)
      try:
        ds_worksheet.write(row,column+5,tbl_row_size[ks][tbl],num_format1)
      except:
        ds_worksheet.write(row,column+5,"no data",num_format1)
      try:
        ds_worksheet.write(row,column+6,t_size/tbl_row_size[ks][tbl],num_format1)
      except:
        ds_worksheet.write(row,column+6,"no data",num_format1)
      row+=1

  ds_worksheet.write(row,column,'Total',header_format4)
  ds_worksheet.write(row,column+2,total_t_size,num_format1)
  ds_worksheet.write(row,column+4,total_set_size,num_format1)

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
#          stats_sheets[sheet_array['sheet_name']] = workbook.add_worksheet(sheet_array['tab_name'])
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
  wl_headers=['Keyspace','Table','Total Reads','Read Calls','Average TPS','% Reads','% RW','','Keyspace','Table','Total Writes','RF','Write Calls','Average TPS','% Writes','% RW']
  wl_headers_width=[14,25,17,13,13,9,9,3,14,25,17,4,17,13,9,9]

  column=0
  for col_width in wl_headers_width:
    worksheet.set_column(column,column,col_width)
    column+=1

  worksheet.merge_range('A1:P1', 'Workload for '+cluster_name, title_format3)
  worksheet.merge_range('A2:G2', 'Reads', title_format)
  worksheet.merge_range('I2:P2', 'Writes', title_format)

  column=0
  for header in wl_headers:
      if header == '':
        worksheet.write(2,column,header)
      else:
        worksheet.write(2,column,header,header_format1)
      column+=1

  row = 3
  perc_reads = 0.0
  column = 0
  for reads in read_count:
    perc_reads = float(read_subtotal) / float(total_reads)
    if (perc_reads <= read_threshold):
      ks = reads['keyspace']
      tbl = reads['table']
      cnt = reads['count']
      try:
        type(table_totals[ks])
      except:
        table_totals[ks] = {}
      table_totals[ks][tbl] = {'reads':cnt,'writes':'n/a'}
      read_subtotal += cnt
      worksheet.write(row,column,ks,data_format)
      worksheet.write(row,column+1,tbl,data_format)
      worksheet.write(row,column+2,cnt,num_format1)
      worksheet.write(row,column+3,float(cnt)/2,num_format1)
      worksheet.write(row,column+4,float(cnt)/total_uptime,num_format2)
      worksheet.write(row,column+5,float(cnt)/total_reads,perc_format)
      worksheet.write(row,column+6,float(cnt)/float(total_rw),perc_format)
      row+=1

  worksheet.write(row,column,'Total',header_format4)
  worksheet.write(row,column+2,read_subtotal,num_format1)
  worksheet.write(row,column+3,read_subtotal/2,num_format1)
  worksheet.write(row+1,column,'TPS',header_format4)
  worksheet.write(row+1,column+2,float(read_subtotal)/float(total_uptime),num_format1)
  worksheet.write(row+1,column+3,float(read_subtotal/2)/float(total_uptime),num_format1)
  worksheet.write(row+2,column,'TP Month',header_format4)
  worksheet.write(row+2,column+2,float(read_subtotal)/float(total_uptime)*60*60*24*365.25/12,num_format1)
  worksheet.write(row+2,column+3,float(read_subtotal/2)/float(total_uptime)*60*60*24*365.25/12,num_format1)

  worksheet.write(row+4,column,'Uptime (sec)',header_format4)
  worksheet.write(row+4,column+1,total_uptime,num_format1)
  worksheet.write(row+5,column,'Uptime (day)',header_format4)
  worksheet.write(row+5,column+1,float(total_uptime)/60/60/24,num_format1)

  perc_writes = 0.0
  row = 3
  column = 8
  astra_write_subtotal = 0
  for writes in write_count:
    perc_writes = float(write_subtotal) / float(total_writes)
    if (perc_writes <= write_threshold):
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
      write_subtotal += cnt
      astra_write_subtotal += float(cnt)/float(tbl_data[ks]['rf'])
      worksheet.write(row,column,ks,data_format)
      worksheet.write(row,column+1,tbl,data_format)
      worksheet.write(row,column+2,cnt,num_format1)
      worksheet.write(row,column+3,tbl_data[ks]['rf'],num_format1)
      worksheet.write(row,column+4,float(cnt)/float(tbl_data[ks]['rf']),num_format1)
      worksheet.write(row,column+5,float(cnt)/total_uptime,num_format2)
      worksheet.write(row,column+6,float(cnt)/total_writes,perc_format)
      worksheet.write(row,column+7,float(cnt)/float(total_rw),perc_format)
      row+=1
  worksheet.write(row,column,'Total',header_format4)
  worksheet.write(row,column+2,write_subtotal,num_format1)
  worksheet.write(row,column+4,astra_write_subtotal,num_format1)
  worksheet.write(row+1,column,'TPS',header_format4)
  worksheet.write(row+1,column+2,float(write_subtotal)/float(total_uptime),num_format1)
  worksheet.write(row+1,column+4,float(astra_write_subtotal)/float(total_uptime),num_format1)
  worksheet.write(row+2,column,'TP Month',header_format4)
  worksheet.write(row+2,column+2,float(write_subtotal)/float(total_uptime)*60*60*24*365.25/12,num_format1)
  worksheet.write(row+2,column+4,float(astra_write_subtotal)/float(total_uptime)*60*60*24*365.25/12,num_format1)


  reads_tps = total_reads/total_uptime
  reads_tpd = reads_tps*60*60*24
  reads_tpmo = reads_tps*60*60*24*365.25/12

  writes_tps = total_writes/total_uptime
  writes_tpd = writes_tps*60*60*24
  writes_tpmo = writes_tps*60*60*24*365.25/12

  astra_writes_tps = astra_total_writes/total_uptime
  astra_writes_tpd = astra_writes_tps*60*60*24
  astra_writes_tpmo = astra_writes_tps*60*60*24*365.25/12

  total_tps = float(total_rw)/total_uptime
  total_tpd = total_tps*60*60*24
  total_tpmo = total_tps*60*60*24*365.25/12
  days_uptime = total_uptime/60/60/24

  row=1
  column=0
  
  worksheet_chart.merge_range('A1:B1', 'Astra Conversion Info for '+cluster_name, title_format3)
  worksheet_chart.set_column(0,0,25)
  worksheet_chart.set_column(1,1,14)
  worksheet_chart.write(row,column,'Read Calls per Sec',title_format4)
  worksheet_chart.write(row,column+1,float(read_subtotal/2)/float(total_uptime),num_format1)
  worksheet_chart.write(row+1,column,'Read Calls per Month',title_format4)
  worksheet_chart.write(row+1,column+1,float(read_subtotal/2)/float(total_uptime)*60*60*24*365.25/12,num_format1)
  worksheet_chart.write(row+2,column,'Write Calls per Sec',title_format4)
  worksheet_chart.write(row+2,column+1,astra_writes_tps,num_format1)
  worksheet_chart.write(row+3,column,'Write Calls per Month',title_format4)
  worksheet_chart.write(row+3,column+1,astra_writes_tpmo,num_format1)
  worksheet_chart.write(row+4,column,'Data Size (GB)',title_format4)
  worksheet_chart.write(row+4,column+1,total_set_size/1000000000,num_format2)

  gc_headers=['Name','Level/DC','Pauses','Max','P99','P98','P95','P90','P75','P50','Min','From','To','Max Date']

  gc_fields=['Name','Level','Pauses','Max','P99','P98','P95','P90','P75','P50','Min','From','To','max_gc']
  gc_widths=[18,14,8,6,6,6,6,6,6,6,6,35,35,17]

  prev_dc=0
  row=0
  column=0
  for header in gc_headers:
    gc_worksheet.write(row,column,header,title_format)
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

