#!/usr/bin/env python3

#pip install xlsxwriter
#pip install Pandas

# get the dc name for each node
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
    exit('ERROR: No Status File - ' + statuspath)

# parse the system.log for the GC info
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

# print the GC percentage
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

def sortFunc(e):
  return e['count']

def write_row(sheet_name,row_data,d_format,blank_col=[]):
  for col_num,data in enumerate(row_data):
    if col_num not in blank_col:
      stats_sheets[sheet_name].write(row[sheet_name],col_num, data, d_format)
  row[sheet_name]+=1

# get param value
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

import os.path
from os import path
import xlsxwriter
import sys
import pandas as pd
import datetime
import re
import zipfile


def schemaTag(writeFile,tagType,level,ks,tbl,cql):
  writeFile.writelines(['  - tags:\n','      phase: '+tagType+'_'+level+'_'+ks])
  if level == 'table':
    writeFile.write('_'+tbl)
  writeFile.writelines(['\n','    statements:\n','      - |\n        '+cql+'\n\n'])

def rwTag(writeFile,rwCQL,ks,tbl,tbl_info,ratio='n'):
  if ratio == 'n':
    writeFile.writelines(['  - tags:\n','      phase: '+rwCQL+'_'+ks+'_'+tbl+'\n'])
  elif ratio == 'y':
    writeFile.writelines(['  - tags:\n','      phase: load_'+rwCQL+'_'+ks+'_'+tbl+'\n'])
    ratio_val = str(int(tbl_info['ratio'][rwCQL]*1000))
    writeFile.writelines(['    params:\n','      ratio: ',ratio_val,'\n'])
  writeFile.writelines(['    statements:\n','      - |\n        '])
  field_array = []
  join_info = '},{'+ks+'_'+tbl+'_'
  if rwCQL == 'read':
    cql = 'SELECT * FROM '+ks+'.'+tbl+' WHERE '
    for fld_name,fld_type in tbl_info['field'].items():
      if (fld_name in tbl_info['pk']):
        field_array.append(fld_name+'={'+ks+'_'+tbl+'_'+fld_name+'}')
    field_info = ' AND '.join(map(str, field_array))
    writeFile.write(cql+field_info+'\n\n')
  elif rwCQL == 'write':
    field_array = tbl_info['field'].keys()
    field_names =  ','.join(map(str, field_array))
    field_values =  join_info.join(map(str, field_array))
    cql = 'INSERT INTO '+ks+'.'+tbl+' ('+field_names+') VALUES ({'+ks+'_'+tbl+'_'+field_values+'})'
    writeFile.write(cql+'\n\n')

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


data_url = []
system_keyspace = ['OpsCenter','dse_insights_local','solr_admin','test','dse_system','dse_analytics','system_auth','system_traces','system','dse_system_local','system_distributed','system_schema','dse_perf','dse_insights','dse_security','killrvideo','dse_leases','dsefs_c4z','HiveMetaStore','dse_analytics','dsefs','spark_system']
wl_headers=['Keyspace','Table','Table Size','','Keyspace','Table','Total Reads','Average TPS','% Reads','% RW','','Keyspace','Table','Total Writes','Average TPS','% Writes','% RW','','TOTALS']
wl_headers_width=[14,25,17,3,14,25,17,13,9,9,3,14,25,17,13,9,9,3,25,20]
ks_type_abbr = {'app':'Application','sys':'System'}
read_threshold = 1
write_threshold = 1
include_yaml = 0
new_dc = ''
show_help = ''
include_system = 0
log_df = '%Y-%m-%d %H:%M:%S'
dt_fmt = '%m/%d/%Y %I:%M%p'
tz = {}

for argnum,arg in enumerate(sys.argv):
  if(arg=='-h' or arg =='--help'):
    show_help = 'y'
  elif(arg=='-p'):
    data_url.append(sys.argv[argnum+1])
  elif(arg=='-rt'):
    read_threshold = float(sys.argv[argnum+1])/100
  elif(arg=='-wt'):
    write_threshold = float(sys.argv[argnum+1])/100
  elif(arg=='-sys'):
    include_system = 1

if (include_system): ks_type_array=['app','sys']
else: ks_type_array=['app']

if show_help:
  help_content = \
  'usage: explore.py [-h] [--help] [-inc_yaml]\n'\
  '                       [-p PATH_TO_DIAG_FOLDER]\n'\
  '                       [-rt READ_THRESHOLD]\n'\
  '                       [-wt WRITE_THRESHOLD]\n'\
  '                       [-sys INCLUDE SYSTEM KEYSPACES]\n'\
  'optional arguments:\n'\
  '-h, --help             This help info\n'\
  '-p                     Path to the diagnostics folder\n'\
  '                        Multiple diag folders accepted\n'\
  '                        i.e. -p PATH1 -p PATH2 -p PATH3\n'\
  '-rt                    Defines percentage of read load\n'\
  '                        to be included in the output\n'\
  '                        Default: 100%\n'\
  '                        i.e. -rt 85\n'\
  '-wt                    Defines percentage of write load\n'\
  '                        to be included in the output\n'\
  '                        Default: 100%\n'\
  '                        i.e. -wt 85\n'
  '-sys                   Include System files in addtional tab\n'\

  exit(help_content)

for cluster_url in data_url:
  cluster_name=''
  is_index = 0
  read_subtotal = {'app':0,'sys':0}
  write_subtotal = {'app':0,'sys':0}
  total_size = {'app':0,'sys':0}
  astra_size = {'app':0,'sys':0}
  dc_total_size = {'app':0,'sys':0}
  total_reads = {'app':0,'sys':0}
  total_writes = {'app':0,'sys':0}
  read_count = {'app':[],'sys':[]}
  write_count = {'app':[],'sys':[]}
  total_rw = {'app':0,'sys':0}
  ks_array = {'app':[],'sys':[]}
  count = 0
  size_table = {'app':{},'sys':{}}
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

  rootPath = cluster_url + '/nodes/'

  # gather dc info
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


  # gather schema
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
            if (ks in system_keyspace): ks_array['sys'].append(ks)
            else: ks_array['app'].append(ks)
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

  for node in os.listdir(rootPath):
    ckpath = rootPath + node + '/nodetool'
    if path.isdir(ckpath):
      iodata = {}
      iodata[node] = {}
      keyspace = ''
      table = ''
      dc = ''
      cfhist = rootPath + node + '/nodetool/cfhistogram'
      tablehist = rootPath + node + '/nodetool/tablehistogram'
      tblhist = 0
      cfstat = rootPath + node + '/nodetool/cfstats'
      tablestat = rootPath + node + '/nodetool/tablestats'
      clusterpath = rootPath + node + '/nodetool/describecluster'
      infopath = rootPath + node + '/nodetool/info'

      if (cluster_name == ''):
        cluster_name = get_param(clusterpath,'Name:',1)
        
#      try:
#        cfhistFile = open(cfhist, 'r')
#      except:
#        cfhistFile = open(tablehist, 'r')

      try:
        cfstatFile = open(cfstat, 'r')
      except:
        cfstatFile = open(tablestat, 'r')


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
            ks_type=''
            if (ks in ks_array['app']): ks_type = 'app'
            elif (ks in ks_array['sys']): ks_type = 'sys'
          elif (ks_type<>''):
            if('Table: ' in line):
              tbl = line.split(':')[1].strip()
              is_index = 0
            elif('Table (index): ' in line):
              tbl = line.split(':')[1].strip()
              is_index = 1
            if(tbl<>''):
              if ('Space used (live): ' in line):
                tsize = float(line.split(':')[1].strip())
                if (tsize > 0):
                  total_size[ks_type] += tsize
                  # astra pricing will be based on data on one set of data
                  # divide the total size by the total rf (gives the size per node)
                  astra_size[ks_type] += tsize / tbl_data[ks]['rf']
                  try:
                    type(size_table[ks_type][ks])
                  except:
                    size_table[ks_type][ks] = {}
                  try:
                    type(size_table[ks_type][ks][tbl])
                    size_table[ks_type][ks][tbl] += tsize
                  except:
                    size_table[ks_type][ks][tbl] = tsize
              if('Local read count: ' in line):
                count = int(line.split(':')[1].strip())
                if (count > 0):
                  total_reads[ks_type] += count
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
                    total_writes[ks_type] += count
                    try:
                      type(write_table[ks])
                    except:
                      write_table[ks] = {}
                    try:
                      type(write_table[ks][tbl])
                      write_table[ks][tbl] += count
                    except:
                      write_table[ks][tbl] = count

  
  for ks,readtable in read_table.items():
    if ks not in system_keyspace and ks != '': ks_type='app'
    else: ks_type='sys'
    for tablename,tablecount in readtable.items():
      read_count[ks_type].append({'keyspace':ks,'table':tablename,'count':tablecount})

  for ks,writetable in write_table.items():
    if ks not in system_keyspace and ks != '': ks_type='app'
    else: ks_type='sys'
    for tablename,tablecount in writetable.items():
      write_count[ks_type].append({'keyspace':ks,'table':tablename,'count':tablecount})

  for ks_type in ks_type_array:
    read_count[ks_type].sort(reverse=True,key=sortFunc)
    write_count[ks_type].sort(reverse=True,key=sortFunc)
    total_rw[ks_type] = total_reads[ks_type]+total_writes[ks_type]
  
  
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

  
  # Get GC Data
  rootPath = cluster_url + '/nodes/'
  for node in os.listdir(rootPath):
    systemlogpath = rootPath + node + '/logs/cassandra/'
    systemlog = systemlogpath + 'system.log'
    jsppath = rootPath + node + '/java_system_properties.json'
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
      tz[node] = get_param(jsppath,'user.timezone',2).strip(',').strip('"')
      if (tz[node]=='Default'): tz[node] = 'UTC'
      
      for logfile in os.listdir(systemlogpath):
        if(logfile.split('.')[0] == 'system'):
          systemlog = systemlogpath + logfile
          cor_node = node.replace('-','.')
          parseGC(cor_node,systemlog,systemlogpath)


  # Additional log path
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

  #cluster GC Percents
  get_gc_data('Cluster',cluster_name,cluster_gcpause,0)

  for dc, dc_pause in dc_gcpause.items():
    get_gc_data('DC',dc,dc_pause,0)
  for node, node_pause in node_gcpause.items():
    get_gc_data('Node',node,node_pause,1)

  #list DC
  for node_val, dc_val in node_dc.items():
    dc_list.append(dc_val)
  dc_list = list(dict.fromkeys(dc_list))
  dc_list.sort()

  #Create Cluster GC Spreadsheet
  worksheet = {}
  workbook = xlsxwriter.Workbook(cluster_url + '/' + cluster_name + '_' + 'astra_chart' + '.xlsx')
  worksheet_chart = workbook.add_worksheet('Astra Chart')

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

  for ks_type in ks_type_array:
    worksheet[ks_type] = workbook.add_worksheet(ks_type_abbr[ks_type] + ' Workload')

    column=0
    for col_width in wl_headers_width:
      worksheet[ks_type].set_column(column,column,col_width)
      column+=1




  cluster_name = ''
  prev_nodes = []
  stat_sheets = {}
  headers = {}
  col_widths = {}
  sheets_record = {}
  stats_sheets = {}
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
          stats_sheets[sheet_array['sheet_name']] = workbook.add_worksheet(sheet_array['tab_name'])
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


      keyspace = ''
      table = ''
      dc = ''

      info = rootPath + node + '/nodetool/info'
      infoFile = open(info, 'r')
      for line in infoFile:
        if('Data Center' in line):
          dc = line.split(':')[1].strip()

      if(node_status):
        proxyhistData[node] = []
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

      cfstat = rootPath + node + '/nodetool/cfstats'
      cfstatFile = open(cfstat, 'r')
      for line in cfstatFile:
        if('Keyspace' in line):
          keyspace = line.split(':')[1].strip()
        elif('Table: ' in line):
          table = line.split(':')[1].strip()
        elif(':' in line):
          header = line.split(':')[0].strip()
          value = line.split(':')[1].strip()
          row_data = [node,dc,keyspace,table,header,value]
 
          for sheet_array in sheets_data:
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

      key_record = {}
      key_data = {}
      for sheet_array in sheets_data:
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
#      exit(key_data)

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

  for ks_type in ks_type_array:
    worksheet[ks_type].merge_range('A1:T1', 'Workload for '+cluster_name, title_format3)
    worksheet[ks_type].merge_range('A2:C2', 'Table Size', title_format)
    worksheet[ks_type].merge_range('E2:J2', 'Reads', title_format)
    worksheet[ks_type].merge_range('L2:Q2', 'Writes', title_format)
    worksheet[ks_type].merge_range('S2:T2', 'Totals', title_format)

  for ks_type in ks_type_array:
    column=0
    for header in wl_headers:
        if header == '':
          worksheet[ks_type].write(2,column,header)
        else:
          worksheet[ks_type].write(2,column,header,header_format1)
        column+=1

  last_row = 0

  for ks_type in ks_type_array:
    row = {'app':3,'sys':3}
    perc_reads = 0.0
    column = 0
    for ks,t_data in size_table[ks_type].items():
      for tbl,t_size in t_data.items():
        worksheet[ks_type].write(row[ks_type],column,ks,data_format)
        worksheet[ks_type].write(row[ks_type],column+1,tbl,data_format)
        worksheet[ks_type].write(row[ks_type],column+2,t_size,num_format1)
        row[ks_type]+=1

    last_row = row[ks_type]

    row = {'app':3,'sys':3}
    perc_reads = 0.0
    column = 4
    for reads in read_count[ks_type]:
      perc_reads = float(read_subtotal[ks_type]) / float(total_reads[ks_type])
      if (perc_reads <= read_threshold):
        ks = reads['keyspace']
        tbl = reads['table']
        cnt = reads['count']
        try:
          type(table_totals[ks])
        except:
          table_totals[ks] = {}
        table_totals[ks][tbl] = {'reads':cnt,'writes':'n/a'}
        read_subtotal[ks_type] += cnt
        worksheet[ks_type].write(row[ks_type],column,ks,data_format)
        worksheet[ks_type].write(row[ks_type],column+1,tbl,data_format)
        worksheet[ks_type].write(row[ks_type],column+2,cnt,num_format1)
        worksheet[ks_type].write(row[ks_type],column+3,float(cnt)/total_uptime,num_format2)
        worksheet[ks_type].write(row[ks_type],column+4,float(cnt)/total_reads[ks_type],perc_format)
        worksheet[ks_type].write(row[ks_type],column+5,float(cnt)/float(total_rw[ks_type]),perc_format)
        row[ks_type]+=1
  
    if (last_row<row[ks_type]): last_row=row[ks_type]

    perc_writes = 0.0
    row = {'app':3,'sys':3}
    column = 11
    for writes in write_count[ks_type]:
      perc_writes = float(write_subtotal[ks_type]) / float(total_writes[ks_type])
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
        write_subtotal[ks_type] += cnt
        worksheet[ks_type].write(row[ks_type],column,ks,data_format)
        worksheet[ks_type].write(row[ks_type],column+1,tbl,data_format)
        worksheet[ks_type].write(row[ks_type],column+2,cnt,num_format1)
        worksheet[ks_type].write(row[ks_type],column+3,float(cnt)/total_uptime,num_format2)
        worksheet[ks_type].write(row[ks_type],column+4,float(cnt)/total_writes[ks_type],perc_format)
        worksheet[ks_type].write(row[ks_type],column+5,float(cnt)/float(total_rw[ks_type]),perc_format)
        row[ks_type]+=1

    if (last_row<row[ks_type]): last_row=row[ks_type]
    if (last_row<16): last_row=16
    worksheet[ks_type].merge_range('A'+str(last_row+3)+':D'+str(last_row+3), 'NOTES', title_format2)
    worksheet[ks_type].merge_range('A'+str(last_row+4)+':D'+str(last_row+4), 'Transaction totals (Reads/Writes) include all nodes (nodetool cfstats)', data_format)
    worksheet[ks_type].merge_range('A'+str(last_row+5)+':D'+str(last_row+5), 'Log Times (which is used to calculate TPS...) is a sum of the uptimes of all nodes', data_format)
    worksheet[ks_type].merge_range('A'+str(last_row+6)+':D'+str(last_row+6), '% RW is the Read or Write % of the total reads and writes', data_format)
    worksheet[ks_type].merge_range('A'+str(last_row+7)+':D'+str(last_row+7), '* TPMO - transactions per month is calculated at 30.4375 days (365.25/12)', data_format)

    reads_tps = total_reads[ks_type]/total_uptime
    reads_tpd = reads_tps*60*60*24
    reads_tpmo = reads_tps*60*60*24*365.25/12

    writes_tps = total_writes[ks_type]/total_uptime
    writes_tpd = writes_tps*60*60*24
    writes_tpmo = writes_tps*60*60*24*365.25/12

    total_tps = float(total_rw[ks_type])/total_uptime
    total_tpd = total_tps*60*60*24
    total_tpmo = total_tps*60*60*24*365.25/12
    days_uptime = total_uptime/60/60/24

    row=1
    column=18
    worksheet[ks_type].write(row+1,column,'Reads',header_format4)
    worksheet[ks_type].write(row+1,column+1,total_reads[ks_type],num_format3)
    worksheet[ks_type].write(row+2,column,'Reads Average TPS',header_format3)
    worksheet[ks_type].write(row+2,column+1,reads_tps,num_format2)
    worksheet[ks_type].write(row+3,column,'Reads Average TPD',header_format3)
    worksheet[ks_type].write(row+3,column+1,reads_tpd,num_format1)
    worksheet[ks_type].write(row+4,column,'Reads Average TPMO*',header_format3)
    worksheet[ks_type].write(row+4,column+1,reads_tpmo,num_format1)
    worksheet[ks_type].write(row+5,column,'Reads % RW',header_format3)
    worksheet[ks_type].write(row+5,column+1,total_reads[ks_type]/float(total_rw[ks_type]),perc_format)
    worksheet[ks_type].write(row+6,column,'Writes',header_format4)
    worksheet[ks_type].write(row+6,column+1,total_writes[ks_type],num_format3)
    worksheet[ks_type].write(row+7,column,'Writes Average TPS',header_format3)
    worksheet[ks_type].write(row+7,column+1,writes_tps,num_format2)
    worksheet[ks_type].write(row+8,column,'Writes Average TPD',header_format3)
    worksheet[ks_type].write(row+8,column+1,writes_tpd,num_format1)
    worksheet[ks_type].write(row+9,column,'Writes Average TPMO*',header_format3)
    worksheet[ks_type].write(row+9,column+1,writes_tpmo,num_format1)
    worksheet[ks_type].write(row+10,column,'Writes % RW',header_format3)
    worksheet[ks_type].write(row+10,column+1,total_writes[ks_type]/float(total_rw[ks_type]),perc_format)
    worksheet[ks_type].write(row+11,column,'Total RW (Reads+Writes)',header_format4)
    worksheet[ks_type].write(row+11,column+1,total_rw[ks_type],num_format3)
    worksheet[ks_type].write(row+12,column,'Total Log Time (Seconds)',header_format3)
    worksheet[ks_type].write(row+12,column+1,total_uptime,num_format1)
    worksheet[ks_type].write(row+13,column,'Total Log Time (Days)',header_format3)
    worksheet[ks_type].write(row+13,column+1,days_uptime,num_format1)
    worksheet[ks_type].write(row+14,column,'Total Average TPS',header_format3)
    worksheet[ks_type].write(row+14,column+1,total_tps,num_format1)
    worksheet[ks_type].write(row+15,column,'Total Average TPD',header_format3)
    worksheet[ks_type].write(row+15,column+1,total_tpd,num_format1)
    worksheet[ks_type].write(row+16,column,'Total Average TPMO*',header_format3)
    worksheet[ks_type].write(row+16,column+1,total_tpmo,num_format1)
    worksheet[ks_type].write(row+17,column,ks_type_abbr[ks_type] + ' Data Size (GB)',header_format4)
    worksheet[ks_type].write(row+17,column+1,total_size[ks_type]/1000000000,num_format3)

    if ks_type=='app':
      
      row=1
      column=0
      
      worksheet_chart.merge_range('A1:B1', 'Astra Conversion Info', title_format3)
      worksheet_chart.set_column(0,0,24)
      worksheet_chart.set_column(1,1,14)
      worksheet_chart.write(row,column,'Read Calls per Sec',title_format4)
      worksheet_chart.write(row,column+1,reads_tps/2,num_format2)
      worksheet_chart.write(row+1,column,'Read Calls per Month',title_format4)
      worksheet_chart.write(row+1,column+1,reads_tpmo/2,num_format1)
      worksheet_chart.write(row+2,column,'Write Calls per Sec',title_format4)
      worksheet_chart.write(row+2,column+1,writes_tps,num_format2)
      worksheet_chart.write(row+3,column,'Write Calls per Month',title_format4)
      worksheet_chart.write(row+3,column+1,writes_tpmo,num_format1)
      worksheet_chart.write(row+4,column,'Data Size (GB)',title_format4)
      worksheet_chart.write(row+4,column+1,astra_size[ks_type]/1000000000,num_format2)


    gc_worksheet = workbook.add_worksheet('GC Pauses')

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
exit();

