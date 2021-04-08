#!/usr/bin/env python3

#pip install xlsxwriter
#pip install pandas

# Astra Perseverance Version
version = "1.0.0"

# Astra guardrail test parameter defaults
tp_mv = 2         # Number of materialized views per table
tp_si = 1        # Number of indexes per table
tp_sai = 8       # Number of storage-attached indexes per table
tp_tblcnt = 175   # Number of tables in a keyspace
tp_colcnt = 45    # Number of columns in a table
tp_lpar = 100     # Partition size (MB)

# Database Heaalth test parameter defaults
tp_rl = 100       # Node read latency (ms)
tp_wl = 100       # Node write latency (ms)
tp_sstbl = 20     # SStable count per node/table
tp_gcp = 800      # Node P99 GC pause time
tp_drm = 100000   # Number of dropped mutations per table

#Astra Guardrails
gr_mv = 2         # Number of materialized views per table
gr_si = 1        # Number of indexes per table
gr_sai = 10       # Number of storage-attached indexes per table
gr_tblcnt = 200   # Number of tables in a keyspace
gr_colcnt = 50    # Number of columns in a table
gr_lpar = 200     # Partition size (MB)


info_box = 'DataStax Perseverance\n'\
              'Version '+version+'\n'\
              'This script is intended to be used as a guide.  Not all guardrails\n'\
              'are included in this sheet. Please view current Astra guardrials at\n'\
              'https://docs.datastax.com/en/astra/docs/datastax-astra-database-limits.html\n\n'\
              'Astra Guardrail Limits\n'\
              ' - '+str(gr_mv)+' materialized views per table\n'\
              ' - '+str(gr_si)+' secondary index per table\n'\
              ' - '+str(gr_sai)+' storage-attached indexes per table\n'\
              ' - '+str(gr_tblcnt)+' tables in the database\n'\
              ' - '+str(gr_colcnt)+' columns in a table\n'\
              ' - '+str(gr_lpar)+'MB Partition\n'\
              ' - Use of UDA and UDF\n'\
              'The following items are analyzed with Astra Perseverance:\n'\
              'Astra Guardrail Test Parameters\n'\
              ' - More than '+str(tp_mv)+' materialized views per table\n'\
              ' - More than '+str(tp_si)+' secondary index per table\n'\
              ' - More than '+str(tp_sai)+' storage-attached indexes per table\n'\
              ' - More than '+str(tp_tblcnt)+' tables in the database\n'\
              ' - More than '+str(tp_colcnt)+' columns in a table\n'\
              ' - Partition size greater than '+str(tp_lpar)+'MB\n'\
              ' - Use of UDA and UDF\n'\
             'Database Health Test Parameters\n'\
              ' - Local table read latency more than '+str(tp_rl)+'ms\n'\
              ' - Local table write latency more than '+str(tp_wl)+'ms\n'\
              ' - Node P99 GC pause time greater than '+str(tp_gcp)+'ms\n'\
              ' - More than '+str(tp_sstbl)+' SSTables per table\n'\
              ' - More than '+str(tp_drm)+' dropped mutations per table\n\n'\
              '*** VALUES THAT HAVE GONE BEYOND THE GUARDRAILS\n'\
              'Supported data in separate spreadsheet tabs'\
 
#
info_box_options = {'width': 500,'height': 600,'x_offset': 10,'y_offset': 10,'font': {'color': '#3A3A42','size': 12}}

# tool imports
import os.path
from os import path
import xlsxwriter
import pandas as pd
import sys
import datetime
import re
import zipfile

# write comment on worksheet field
def write_cmt(wksht,coord,title,vis=0):
  for cmt_array in comments:
    if title in cmt_array['fields']:
      wksht.write_comment(coord,cmt_array['comment'][0],{'visible':vis,'font_size': 12,'x_scale': 2,'y_scale': 2})

# check for guardrail
def add_tp_tbl(gr,ks,tbl,src_ks,src_tbl):
  if src_ks not in system_keyspace:
    try:
      type(tp_tbl_data[gr][src_ks])
    except:
      tp_tbl_data[gr][src_ks]={}
    try:
      type(tp_tbl_data[gr][src_ks][src_tbl])
    except:
      tp_tbl_data[gr][src_ks][src_tbl] = []
    if (ks+'.'+tbl) not in tp_tbl_data[gr][src_ks][src_tbl]:
      tp_tbl_data[gr][src_ks][src_tbl].append(ks+'.'+tbl)


def extract_ip(ip_text):
  ip_add = []
  ips = re.findall(r'[0-9]+(?:\.[0-9]+){3}', ip_text)
  for ip in ips:
          ip_add.append(ip)
  return ''.join(ip_add)

# the node_ip is created in case the directory name (or node) is not the ip address
# this is specifically used for adding the node uptime on the node tab
def find_ip_addr(node,node_path):
  systemlog = rootPath + node_path + '/logs/cassandra/system.log'
  if path.isfile(systemlog):
    systemlogFile = open(systemlog, 'r')
    for line in systemlogFile:
      if node in line:
        try:
          ip_text =line.split(node)[1].split()[0].strip('/')
          ip_adr = extract_ip(ip_text)
          node_ip[node]=ip_adr
          systemlogFile.close()
          return 0
        except:
          cont=1
    systemlogFile.close()
  else:
    exit('The system log file for node ' + node_path + ' is not available (' + systemlog +')')
  return 1

# collect the dc name for each node
def get_dc(rootPath,statuspath,node):
  if(path.exists(statuspath)):
    statusFile = open(statuspath, 'r')
    dc = ''
    next_ip=1
    for line in statusFile:
      if('Datacenter:' in line):
        dc = str(line.split(':')[1].strip())
        try:
          type(node_status_data[dc])
        except:
          node_status_data[dc]={}
        if dc not in dc_array:
          dc_array.append(dc)
          dc_gcpause[dc]=[]
          newest_gc[dc]={'jd':0.0,'dt':''}
          oldest_gc[dc]={'jd':99999999999.9,'dt':''}
          max_gc[dc]=''
      if line.count('.')>=3:
        ip_addr = line.split()[1]
        if ip_addr in ip_node:
          node_name = ip_node[ip_addr]
          try:
            type(node_dc[node_name])
          except:
            node_dc[node_name]=dc
          try:
            type(node_status_data[dc][node_name])
          except:
            values = line.split();
            node_status_data[dc][node_name] = {'Load':values[2] + ' ' + values[3],'Tokens':int(values[4]),'Rack':values[7]}
        else:
          try:
            type(warnings['Missing Node Data'])
          except:
            warnings['Missing Data']={'Missing Node Data':[]}
          warn_val=ip_addr
          if warn_val not in warnings['Missing Data']['Missing Node Data']:
            warnings['Missing Data']['Missing Node Data'].append(warn_val)
          
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
      try:
        log_jd = pd.Timestamp(year=log_dt.year,month=log_dt.month,day=log_dt.day,hour=log_dt.hour,minute=log_dt.minute,tz=tz[node]).to_julian_date()
      except:
        exit('Installation of Pandas required')
      database_gcpause.append(int(gcpause))
      dc_gcpause[dc].append(int(gcpause))
      node_gcpause[node].append(int(gcpause))
      if(newest_gc[database_name]['jd']<log_jd): newest_gc[database_name]={'jd':log_jd,'dt':ldatetime + ' ' + tz[node]}
      if(oldest_gc[database_name]['jd']>log_jd): oldest_gc[database_name]={'jd':log_jd,'dt':ldatetime + ' ' + tz[node]}
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
      if (level=='Database' and gcpause[p99_pos] > tp_gcp):
        warnings['Database Health']['GC Pauses']=['P99 GC pause greater than '+str(int(tp_gcp))]
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
      try:
        if (sheet_name=='ph' or sheet_name=='rlatency'  or sheet_name=='wlatency'):
          stats_sheets[sheet_name].write(row[sheet_name],col_num, float(data.strip('ms').strip()), num_format2)
        else:
          stats_sheets[sheet_name].write(row[sheet_name],col_num, int(data), num_format1)
      except:
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

# communicate command line help
for argnum,arg in enumerate(sys.argv):
  if(arg=='-h' or arg =='--help'):
    help_content = \
      'usage: look.py [-h] [--help] [-inc_yaml]\n'\
      '                       [-p PATH_TO_DIAG_FOLDER]\n'\
      '                       [-tp_tblcnt CLUSTER_TABLE_COUNT_GUARDRAIL]\n'\
      '                       [-tp_mv MATERIALIZED_VIEW_GUARDRAIL]\n'\
      '                       [-tp_si SECONDARY INDEX_GUARDRAIL]\n'\
      '                       [-tp_sai STORAGE_ATTACHED_INDEX_GUARDRAIL]\n'\
      '                       [-tp_lpar LARGE_PARTITON_SIZE_GUARDRAIL]\n'\
      '                       [-tp_rl READ_LATENCY_THRESHOLD]\n'\
      '                       [-tp_wl WRITE_LATENCY_THRESHOLD]\n'\
      '                       [-tp_sstbl SSTABLE_COUNT_THRESHOLD]\n'\
      '                       [-tp_drm DROPPED_MUTATIONS_COUNT_THRESHOLD]\n'\
      'required arguments:\n'\
      '-p                     Path to the diagnostics folder\n'\
      '                        Multiple diag folders accepted\n'\
      '                        i.e. -p PATH1 -p PATH2 -p PATH3\n'\
      'optional arguments:\n'\
      '-v, --version          Version\n'\
      '-h, --help             This help info\n'\
      '-tp_tblcnt             Database Table Count (Guardrail)\n'\
      '                        Number of tables in the database\n'\
      '                        to be listed in the Number of Tables tab\n'\
      '                        Astra Guardrail Limit: '+str(gr_tblcnt)+'\n'\
      '                        Test Parameter: >'+str(tp_tblcnt)+'\n'\
      '-tp_colcnt             Table Column Count (Guardrail)\n'\
      '                        Number of columns in a table\n'\
      '                        Astra Guardrail Limit: '+str(gr_colcnt)+'\n'\
      '                        Test Parameter: >'+str(tp_colcnt)+'\n'\
      '-tp_mv                 Materialized Views  (Guardrail)\n'\
      '                        Number of Materialized Views of a table\n'\
      '                        Astra Guardrail Limit: '+str(gr_mv)+'\n'\
      '                        Test Parameter: >'+str(tp_mv)+'\n'\
      '-tp_si                Secondary Indexes  (Guardrail)\n'\
      '                        Number of Secondary Indexes of a table\n'\
      '                        Astra Guardrail Limit: '+str(gr_si)+'\n'\
      '                        Test Parameter: >'+str(tp_si)+'\n'\
      '-tp_sai                Storage Attached Indexes  (Guardrail)\n'\
      '                        Number of SAI of a table\n'\
      '                        Astra Guardrail Limit: '+str(gr_sai)+'\n'\
      '                        Test Parameter: >'+str(tp_sai)+'\n'\
      '-tp_lpar               Large Partitions (Guardrail)\n'\
      '                        Size of partition in MB\n'\
      '                        to be listed in the Large Partition tab\n'\
      '                        Astra Guardrail Limit: '+str(gr_lpar)+'MB\n'\
      '                        Test Parameter: >'+str(tp_lpar)+'\n'\
      '-tp_rl                 Local Read Latency (Database Health)\n'\
      '                        Local read time(ms) in the cfstats log \n'\
      '                        to be listed in the Read Latency tab\n'\
      '                        Test Parameter: >'+str(tp_rl)+'\n'\
      '-tp_wl                 Local Write Latency (Database Health)\n'\
      '                        Local write time(ms) in the cfstats log \n'\
      '                        to be listed in the Read Latency tab\n'\
      '                        Test Parameter: >'+str(tp_wl)+'\n'\
      '-tp_sstbl              SSTable Count (Database Health)\n'\
      '                        SStable count in the cfstats log \n'\
      '                        to be listed in the Table Qantity tab\n'\
      '                        Test Parameter: >'+str(tp_sstbl)+'\n'\
      '-tp_drm                Dropped Mutations (Database Health)\n'\
      '                        Dropped Mutation count in the cfstats log \n'\
      '                        to be listed in the Dropped Mutation tab\n'\
      '                        Test Parameter: >'+str(tp_drm)+'\n\n'\
      '-tp_gcp                GCPauses (Database Health)\n'\
      '                        Node P99 GC pause time (ms)\n'\
      '                        to be listed in the GC Pauses tab\n'\
      '                        Test Parameter: >'+str(tp_gcp)+'\n\n'\
      'Notice: Test parameters cannot be larger than guardrails'
    exit(help_content)
  elif(arg=='-v' or arg =='--version'):
    exit("Version " + version)

# collect and analyze command line arguments
for argnum,arg in enumerate(sys.argv):
  if(arg=='-p'):
    data_url.append(sys.argv[argnum+1])
  elif(arg=='-tp_rl'):
    tp_rl = float(sys.argv[argnum+1])
  elif(arg=='-tp_wl'):
    tp_wl = float(sys.argv[argnum+1])
  elif(arg=='-tp_sstbl'):
    tp_sstbl = float(sys.argv[argnum+1])
  elif(arg=='-tp_drm'):
    tp_drm = float(sys.argv[argnum+1])
  elif(arg=='-tp_lpar'):
    if int(sys.argv[argnum+1]) <= gr_lpar:
      tp_lpar = float(sys.argv[argnum+1])
  elif(arg=='-tp_gcp'):
    tp_gcp = float(sys.argv[argnum+1])
  elif(arg=='-tp_tblcnt'):
    if int(sys.argv[argnum+1]) <= gr_tblcnt:
      tp_tblcnt = float(sys.argv[argnum+1])
  elif(arg=='-tp_colcnt'):
    if int(sys.argv[argnum+1]) <= gr_colcnt:
      tp_colcnt = float(sys.argv[argnum+1])
  elif(arg=='-tp_mv'):
    if int(sys.argv[argnum+1]) <= gr_mv:
      tp_mv = float(sys.argv[argnum+1])
  elif(arg=='-tp_si'):
    if int(sys.argv[argnum+1]) <= gr_si:
      tp_si = float(sys.argv[argnum+1])
  elif(arg=='-tp_sai'):
    if int(sys.argv[argnum+1]) <= gr_sai:
      tp_sai = float(sys.argv[argnum+1])



# Organize primary support tab information
sheets_data = []
sheets_data.append({'sheet_name':'node','tab_name':'Node Data','freeze_row':1,'freeze_col':0,'cfstat_filter':'','headers':['Datacenter','Node','Load','Tokens','Rack','Uptime (sec)','Uptime'],'widths':[14,30,14,8,11,15,15],'extra':0,'comment':'','tp_type':''})
sheets_data.append({'sheet_name':'ph','tab_name':'Proxihistogram','freeze_row':2,'freeze_col':0,'cfstat_filter':'','headers':['Datacenter','Node','Max','P99','P98','P95','P75','P50','Min','','Datacenter','Node','Max','P99','P98','P95','P75','P50','Min'],'widths':[20,20,10,10,10,10,10,10,10,3,20,20,10,10,10,10,10,10,10],'extra':0,'comment':'','tp_type':''})
sheets_data.append({'sheet_name':'dmutation','tab_name':'Dropped Mutation','freeze_row':1,'freeze_col':0,'cfstat_filter':'Dropped Mutations','headers':['Node','DC','Keyspace','Table','Dropped Mutations'],'widths':[18,14,14,25,20],'filter_type':'>=','filter':tp_drm,'strip':'','extra':0,'comment':'Tables with more than '+str(tp_drm)+' dropped mutations (cfstats)','tp_type':'drm'})
sheets_data.append({'sheet_name':'numTables','tab_name':'Number of Tables','freeze_row':1,'freeze_col':0,'cfstat_filter':'Total number of tables','headers':['Sample Node','DC','Keyspace','Table','Total Number of Tables'],'widths':[18,14,14,25,23],'filter_type':'>=','filter':tp_tblcnt,'strip':'','extra':1,'comment':'','tp_type':'tblcnt'})
sheets_data.append({'sheet_name':'partition','tab_name':'Large Partitions','freeze_row':1,'freeze_col':0,'cfstat_filter':'Compacted partition maximum bytes','headers':['Node','DC','Keyspace','Table','Partition Size(MB)'],'widths':[18,14,14,25,18],'filter_type':'>=','filter':tp_lpar*1000000,'strip':'','extra':0,'comment':'Table with partiton sizes greater than '+str(tp_lpar)+' (cfstats)','tp_type':'lpar'})
sheets_data.append({'sheet_name':'sstable','tab_name':'SSTable Count','freeze_row':1,'freeze_col':0,'cfstat_filter':'SSTable count','headers':['Example Node','DC','Keyspace','Table','SSTable Count'],'widths':[18,14,14,25,15],'filter_type':'>=','filter':tp_sstbl,'strip':'','extra':1,'comment':'','tp_type':'sstbl'})
sheets_data.append({'sheet_name':'rlatency','tab_name':'Read Latency','freeze_row':1,'freeze_col':0,'cfstat_filter':'Local read latency','headers':['Node','DC','Keyspace','Table','Read Latency (ms)'],'widths':[18,14,14,25,20],'filter_type':'>=','filter':tp_rl,'strip':'ms','extra':0,'comment':'','tp_type':'rl'})
sheets_data.append({'sheet_name':'wlatency','tab_name':'Write Latency','freeze_row':1,'freeze_col':0,'cfstat_filter':'Local write latency','headers':['Node','DC','Keyspace','Table','Write Latency (ms)'],'widths':[18,14,14,25,20],'filter_type':'>=','filter':tp_wl,'strip':'ms','extra':0,'comment':'','tp_type':'wl'})
#sheets_data.append({'sheet_name':'ts','tab_name':'Tombstones','headers':['Node','DC','Keyspace','Table','Write Latency (ms)'],'extra':0})

tp_tbl_data = {
    'Materialized Views':{},
    'Secondary Indexes':{},
    'Storage-Attached Indexes':{}
}
gr_types={
  'Materialized Views':{'gr':gr_mv,'tp':tp_mv,'sup_tab':1},
  'Secondary Indexes':{'gr':gr_si,'tp':tp_si,'sup_tab':1},
  'Storage-Attached Indexes':{'gr':gr_sai,'tp':tp_sai,'sup_tab':1},
  'Number of Tables':{'gr':gr_tblcnt,'tp':tp_tblcnt,'sup_tab':0},
  'Number of Columns':{'gr':gr_colcnt,'tp':tp_colcnt,'sup_tab':0},
  'Large Partitions':{'gr':gr_lpar,'tp':tp_lpar,'sup_tab':0}
}
#do not include keyspaces
dni_keyspace = ['OpsCenter']

#system keyspaces
system_keyspace = ['OpsCenter','dse_insights_local','solr_admin','test','dse_system','dse_analytics','system_auth','system_traces','system','dse_system_local','system_distributed','system_schema','dse_perf','dse_insights','dse_security','dse_system','killrvideo','dse_leases','dsefs_c4z','HiveMetaStore','dse_analytics','dsefs','spark_system']
ks_type_abbr = {'app':'Application','sys':'System'}

comments = [
{
'fields':['Data Size (GB)','Data Set Size'],
'comment':["Data Size is a single set of complete data.  It does not include replicated data across the database"]
},{
'fields':['Read Requests'],
'comment':["The number of read requests during the nodes uptime, analogous to client reads."]
},{
'fields':['Write Requests'],
'comment':["The number of write requests during the nodes uptime, analogous to client writes."]
},{
'fields':['% Reads'],
'comment':["The table's pecentage of the total read requests in the database. (See comment in READ TPS)"]
},{
'fields':['% Writes'],
'comment':["The table's pecentage of the total write requests in the database."]
},{
'fields':['R % RW'],
'comment':["The table's pecentage of read requests of the total RW requests (read and Write) in the database. (See comment in READ TPS)"]
},{
'fields':['W % RW'],
'comment':["The table's pecentage of write requests of the total RW requests (read and Write) in the database. (See comment in READ TPS)"]
},{
'fields':['Average TPS'],
'comment':["The table's read or write request count divided by the uptime. (See comment in READ TPS)"]
},{
'fields':['Read TPS'],
'comment':["The database's average read requests per second based on a local read consistancy level.  The time is determined by the node's uptime."]
},{
'fields':['Read TPMo'],
'comment':["The database's average read requests per month (See comment in READ TPS). The month is calculated at 365.25/12 days."]
},{
'fields':['Write TPS'],
'comment':["The number of write requests per second on the coordinator nodes, analogous to client writes. The time is determined by the node's uptime."]
},{
'fields':['Write TPMo'],
'comment':["The database's average write requests per month. The month is calculated at 365.25/12 days."]
},{
'fields':['Total R % RW'],
'comment':["The total read requests percentage of combined RW requests (read and write) in the database. (See comment in READ TPS)"]
},{
'fields':['Total W % RW'],
'comment':["The total write requests percentage of combined RW requests (read and write) in the database. (See comment in READ TPS)"]
}
]


# run through each database diag file path listed in command line
for database_url in data_url:

  # initialize database vaariables
  database_name=''
  is_index = 0
  read_subtotal = 0
  write_subtotal = 0
  total_size = 0
  dc_total_size = 0
  total_reads = 0
  total_writes = 0
  read_count = []
  write_count = []
  table_count = []
  field_count = {}
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
  node_dc = {}
  dc_list = []
  exclude_tab = []
  node_uptime = {}
  database_gcpause = []
  dc_gcpause = {}
  node_gcpause = {}
  gc_data = {}
  gc_dt = []
  wname = 'gc_data'
  newest_gc = {}
  oldest_gc = {}
  max_gc = {}
  node_ip = {}
  ip_node = {}
  node_status_data = {}
  row={}

  warnings = {'Astra Guardrails':{},'Database Health':{}}

  rootPath = database_url + '/nodes/'

  # collect node info
  if len(node_ip)==0:
    for node_path in os.listdir(rootPath):
      nodetoolpath = rootPath + node_path + '/nodetool'
      if path.exists(nodetoolpath):
        node = extract_ip(node_path)
        if (node==''):
          node=node_path
        statuspath = rootPath + node_path + '/nodetool/status'
        if(path.exists(statuspath)):
          statusFile = open(statuspath, 'r')
          for line in statusFile:
            if line.count('.')>=3:
              ip_addr = line.split()[1]
              if ip_addr==node:
                node_ip[node]=ip_addr
                next_ip=0
              elif ip_addr==node.replace('_','.'):
                node_ip[node]=ip_addr
                next_ip=0
              elif ip_addr==node.replace('-','.'):
                node_ip[node]=ip_addr
                next_ip=0

    for node_path in os.listdir(rootPath):
      statuspath = rootPath + node_path + '/nodetool/status'
      if path.exists(statuspath):
        node = extract_ip(node_path)
        if (node==''):
          node=node_path
        if node not in node_ip:
          find_ip_addr(node,node_path)
    statusFile.close()

  # the ip_node is created in case the directory name (or node) is not the ip address
  # this is specifically used for adding the node uptime on the node tab
  for node_name,ip_name in list(node_ip.items()):
    if ip_name == '':
      del node_ip[node_name]
    else:
      ip_node[ip_name]=node_name

  # collect dc info
  for node_path in os.listdir(rootPath):
    nodetoolpath = rootPath + node_path + '/nodetool'
    if path.exists(nodetoolpath):
      node = extract_ip(node_path)
      if (node==''):
        node=node_path
      if node in node_ip:
        ckpath = rootPath + node_path + '/nodetool'
        if path.isdir(ckpath):
          statuspath = rootPath + node_path + '/nodetool/status'
          get_dc(rootPath,statuspath,node)
          if database_name == '':
            databasepath = rootPath + node_path + '/nodetool/describecluster'
            database_name = get_param(databasepath,'Name:',1)
            newest_gc[database_name]={'jd':0.0,'dt':''}
            oldest_gc[database_name]={'jd':99999999999.9,'dt':''}
            max_gc[database_name]=''

      schemapath = rootPath + node_path + '/driver'
      if path.isdir(schemapath):
        try:
          schemaFile = open(schemapath + '/schema', 'r')
        except:
          exit('Error: No schema file - ' + schemapath + '/schema')

  # collect and analyze schema
  ks = ''
  dc_ks_rf = {}
  prev_node = ''
  is_nodes=0

  for node_path in os.listdir(rootPath):
    nodetoolpath = rootPath + node_path + '/nodetool'
    if path.exists(nodetoolpath):
      node = extract_ip(node_path)
      if (node==''):
        node=node_path
      if node in node_ip:
        if (prev_node==''):
          if (ks==''):
            is_nodes=1
            prev_node=node
            ks = ''
            tbl = ''
            create_stmt = {}
            tbl_data = {}
            schemapath = rootPath + node_path + '/driver'
            if path.isdir(schemapath):
              schemaFile = open(schemapath + '/schema', 'r')
              for line in schemaFile:
                line = line.strip('\n').strip()
                if (line==''): tbl=''
                if("CREATE KEYSPACE" in line):
                  cur_rf = 0
                  prev_ks = ks
                  ks = line.split()[2].strip('"')
                  tbl_data[ks] = {'cql':line,'rf':0}
                  rf=0;
                  if ks not in dni_keyspace:
                    for dc_name in dc_array:
                      if ("'"+dc_name+"':" in line):
                        i=0
                        for prt in line.split():
                          prt_chk = "'"+dc_name+"':"
                          if (prt==prt_chk):
                            rf=line.split()[i+1].strip('}').strip(',').strip("'")
                            try:
                              type(dc_ks_rf[dc_name])
                            except:
                              dc_ks_rf[dc_name] = {}
                            try:
                              type(dc_ks_rf[dc_name][ks])
                            except:
                              dc_ks_rf[dc_name][ks] = rf
                            tbl_data[ks]['rf']+=float(rf)
                          i+=1
                      elif("'replication_factor':" in line):
                        i=0
                        for prt in line.split():
                          prt_chk = "'replication_factor':"
                          if (prt==prt_chk):
                            rf=line.split()[i+1].strip('}').strip(',').strip("'")
                            try:
                              type(dc_ks_rf[dc_name])
                            except:
                              dc_ks_rf[dc_name] = {}
                            try:
                              type(dc_ks_rf[dc_name][ks])
                            except:
                              dc_ks_rf[dc_name][ks] = rf
                            tbl_data[ks]['rf']+=float(rf)
                          i+=1
                      else:tbl_data[ks]['rf']=float(1)
                if ks not in dni_keyspace:
                  if('CREATE INDEX' in line):
                    prev_tbl = tbl
                    tbl = line.split()[2].strip('"')
                    tbl_data[ks][tbl] = {'type':'Index', 'cql':line}
                    src_ks = line.split('ON')[1].split('.')[0].strip().strip('"')
                    src_tbl = line.split('ON')[1].split('.')[1].split()[0].strip()
                    add_tp_tbl('Secondary Indexes',ks,tbl,src_ks,src_tbl)
                    tbl=''
                  elif('CREATE CUSTOM INDEX' in line):
                    prev_tbl = tbl
                    tbl = line.split()[3].strip('"')
                    tbl_data[ks][tbl] = {'type':'Storage-Attached Index', 'cql':line}
                    src_ks = line.split('ON')[1].split('.')[0].strip().strip('"')
                    src_tbl = line.split('ON')[1].split('.')[1].split()[0].strip()
                    add_tp_tbl('Storage-Attached Indexes',ks,tbl,src_ks,src_tbl)
                    tbl=''
                  elif('CREATE TYPE' in line):
                    prev_tbl = tbl
                    tbl_line = line.split()[2].strip('"')
                    tbl = tbl_line.split('.')[1].strip().strip('"')
                    tbl_data[ks][tbl] = {'type':'Type', 'cql':line}
                    tbl_data[ks][tbl]['field'] = {}
                  elif('CREATE AGGREGATE' in line):
                    prev_tbl = tbl
                    if 'IF NOT EXISTS' in line:
                      tbl = line.split()[2].strip('"')
                    else:
                      tbl = line.split()[5].strip('"')
                    tbl_data[ks][tbl] = {'type':'UDA', 'cql':line}
                    tbl_data[ks][tbl]['field'] = {}
                    try:
                      warnings['Astra Guardrails']['User-Defined Aggregate'].append = 'UDA '+tbl+' in '+ks
                    except:
                      warnings['Astra Guardrails']['User-Defined Aggregate'] = ['UDA '+tbl+' in '+ks]
                  elif('CREATE OR REPLACE FUNCTION' in line):
                    prev_tbl = tbl
                    tbl = line.split()[4].strip('"')
                    tbl_data[ks][tbl] = {'type':'UDF', 'cql':line}
                    tbl_data[ks][tbl]['field'] = {}
                    try:
                      warnings['Astra Guardrails']['User-Defined Function'].append = 'UDF '+tbl+' in '+ks
                    except:
                      warnings['Astra Guardrails']['User-Defined Function'] = ['UDF '+tbl+' in '+ks]
                  elif('CREATE TABLE' in line):
                    prev_tbl = tbl
                    tbl_line = line.split()[2].strip('"')
                    tbl = tbl_line.split('.')[1].strip().strip('"')
                    tbl_data[ks][tbl] = {'type':'Table', 'cql':line}
                    tbl_data[ks][tbl]['field'] = {}
                  elif('CREATE MATERIALIZED VIEW' in line ):
                    prev_tbl = tbl
                    tbl_line = line.split()[3].strip('"')
                    tbl = tbl_line.split('.')[1].strip().strip('"')
                    tbl_data[ks][tbl] = {'type':'Materialized Views', 'cql':line}
                    tbl_data[ks][tbl]['field'] = {}
                  if (tbl !=''):
                    if('FROM' in line and tbl_data[ks][tbl]['type']=='Materialized Views'):
                      src_ks = line.split('.')[0].split()[1].strip('"')
                      src_tbl = line.split('.')[1].strip('"')
                      add_tp_tbl('Materialized Views',ks,tbl,src_ks,src_tbl)
                    elif('PRIMARY KEY' in line):
                      if(line.count('(') == 1):
                        tbl_data[ks][tbl]['pk'] = [line.split('(')[1].split(')')[0].split(', ')[0]]
                        tbl_data[ks][tbl]['cc'] = line.split('(')[1].split(')')[0].split(', ')
                        del tbl_data[ks][tbl]['cc'][0]
                      elif(line.count('(') == 2):
                        tbl_data[ks][tbl]['pk'] = line.split('(')[2].split(')')[0].split(', ')
                        tbl_data[ks][tbl]['cc'] = line.split('(')[2].split(')')[1].lstrip(', ').split(', ')
                      tbl_data[ks][tbl]['cql'] += ' ' + line.strip()
                    elif line.strip() != ');':
                      try:
                        tbl_data[ks][tbl]['cql'] += ' ' + line
                        if('AND ' not in line and ' WITH ' not in line):
                          fld_name = line.split()[0]
                          fld_type = line.split()[1].strip(',')
                          if (fld_name!='CREATE'):
                            tbl_data[ks][tbl]['field'][fld_name]=fld_type
                      except:
                        print(('Error1:' + ks + '.' + tbl + ' - ' + line))
  if (is_nodes==0):
    exit('No Node Info')

  # begin looping through each node and collect node info
  tbl_row_size = {}
  for node_path in os.listdir(rootPath):
    nodetoolpath = rootPath + node_path + '/nodetool'
    if path.exists(nodetoolpath):
      node = extract_ip(node_path)
      if (node==''):
        node=node_path
      if node in node_ip:
        # initialize node variables
        iodata = {}
        iodata[node] = {}
        keyspace = ''
        table = ''
        dc = ''
        cfstat = rootPath + node_path + '/nodetool/cfstats'
        tablestat = rootPath + node_path + '/nodetool/tablestats'
        databasepath = rootPath + node_path + '/nodetool/describecluster'
        infopath = rootPath + node_path + '/nodetool/info'

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
            if 'Keyspace' in line:
              ks = line.split(':')[1].strip()
            if ks!='' and ks not in dni_keyspace:
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
              if(tbl!=''):
                try:
                  type(table_tps[ks][tbl])
                except:
                  table_tps[ks][tbl]={'write':0,'read':0}
                if ('Space used (live):' in line):
                  try:
                    tsize = float(line.split(':')[1].strip()) / float(dc_ks_rf[node_dc[node]][ks])
                  except:
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
                  try:
                    count = int(line.split(':')[1].strip()) / float(dc_ks_rf[node_dc[node]][ks])
                  except:
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
                if('Local write count: ' in line):
                  try:
                    count = int(line.split(':')[1].strip()) / float(dc_ks_rf[node_dc[node]][ks])
                  except:
                    count = int(line.split(':')[1].strip())
                  if (count > 0):
                    table_tps[ks][tbl]['write'] += float(count) / float(node_uptime[node])
                    try:
                      total_writes += count
                    except:
                      total_writes = count
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
  for ks,readtable in list(read_table.items()):
    for tablename,tablecount in list(readtable.items()):
      read_count.append({'keyspace':ks,'table':tablename,'count':tablecount})
  for ks,writetable in list(write_table.items()):
    for tablename,tablecount in list(writetable.items()):
      try:
        write_count.append({'keyspace':ks,'table':tablename,'count':tablecount})
      except:
        write_count.append({'keyspace':ks,'table':tablename,'count':tablecount})

  # total up data size across all nodes
  for ks,sizetable in list(size_table.items()):
    for tablename,tablesize in list(sizetable.items()):
      table_count.append({'keyspace':ks,'table':tablename,'count':tablesize})

  # sort R/W data
  read_count.sort(reverse=True,key=sortFunc)
  write_count.sort(reverse=True,key=sortFunc)
  table_count.sort(reverse=True,key=sortFunc)
  total_rw = total_reads+total_writes
    
  # collect GC Data
  rootPath = database_url + '/nodes/'
  for node_path in os.listdir(rootPath):
    nodetoolpath = rootPath + node_path + '/nodetool'
    if path.exists(nodetoolpath):
      node = extract_ip(node_path)
      if (node==''):
        node=node_path
      if node in node_ip:
        systemlogpath = rootPath + node_path + '/logs/cassandra/'
        systemlog = systemlogpath + 'system.log'
        jsppath1 = rootPath + node_path + '/java_system_properties.json'
        jsppath2 = rootPath + node_path + '/java_system_properties.txt'
        infopath = rootPath + node_path + '/nodetool/info'
        if(path.exists(systemlog)):
          statuspath = rootPath + node_path + '/nodetool/status'
          node_gcpause[node] = []
          newest_gc[node]={'jd':0.0,'dt':''}
          oldest_gc[node]={'jd':99999999999.9,'dt':''}
          max_gc[node]=''
          tz[node]='UTC'
          for logfile in os.listdir(systemlogpath):
            if(logfile.split('.')[0] == 'system'):
              systemlog = systemlogpath + '/' + logfile
              parseGC(node,systemlog,systemlogpath)

  # collect GC data from additional log path
  addlogs = './AdditionalLogs'
  if(path.exists(addlogs)):
    for node_path in os.listdir(rootPath):
      nodetoolpath = rootPath + node_path + '/nodetool'
      if path.exists(nodetoolpath):
        node = extract_ip(node_path)
        if (node==''):
          node=node_path
        if node in node_ip:
          dirpath = 'AdditionalLogs/' + node_path
          if(node.split('-')[0]=='10'):
            logdir = 'AdditionalLogs/' + node_path + '/var/log/cassandra'
            for logfile in os.listdir(logdir):
              if(logfile.split('.')[0] == 'system'):
                systemlogpath = logdir + '/'
                systemlog = systemlogpath + '/' + logfile
                cor_node = node.replace('-','.')
                parseGC(cor_node,systemlog,systemlogpath)

  #collect database GC Percents
  get_gc_data('Database',database_name,database_gcpause,0)

  for dc, dc_pause in list(dc_gcpause.items()):
    get_gc_data('DC',dc,dc_pause,0)
  for node, node_pause in list(node_gcpause.items()):
    get_gc_data('Node',node,node_pause,1)

  # Create DC List
  for node_val, dc_val in list(node_dc.items()):
    dc_list.append(dc_val)
  dc_list = list(dict.fromkeys(dc_list))
  dc_list.sort()


  # Astra Guardrails
  gr = 'Astra Guardrails'
  for tp_name, ks_array in list(tp_tbl_data.items()):
    gr_lmt = gr_types[tp_name]['gr']
    tp_lmt = gr_types[tp_name]['tp']
    if gr_types[tp_name]['sup_tab']:
      try: type(warnings[gr][tp_name])
      except: warnings[gr][tp_name] = []
      for ks,tbl_array in list(ks_array.items()):
        if ks not in system_keyspace:
          for tbl,tp_array in list(tbl_array.items()):
            if len(tp_array)>gr_lmt:
              warnings[gr][tp_name].append(str(len(tp_array))+' '+tp_name+' of '+ks+'.'+tbl+'***')
            elif len(tp_array)>tp_lmt:
              warnings[gr][tp_name].append(str(len(tp_array))+' '+tp_name+' of '+ks+'.'+tbl)



  # review column count
  gr_lmt = gr_types['Number of Columns']['gr']
  tp_lmt = gr_types['Number of Columns']['tp']
  for ks,ks_array in list(tbl_data.items()):
    if (ks not in system_keyspace):
      for tbl,tbl_array in list(ks_array.items()):
        if (tbl!='cql' and tbl!='rf'):
          for tbl_prt,prt_array in list(tbl_array.items()):
            if tbl_prt=='field':
              if len(prt_array)>gr_lmt:
                try:
                  warnings[gr]['Number of Columns'].append = str(tp_colcnt)+' columns in '+ks+'.'+tbl+'***'
                except:
                  warnings[gr]['Number of Columns'] = [str(len(prt_array))+' columns in '+ks+'.'+tbl+'***']
              elif len(prt_array)>tp_lmt:
                try:
                  warnings[gr]['Number of Columns'].append = str(tp_colcnt)+' columns in '+ks+'.'+tbl
                except:
                  warnings[gr]['Number of Columns'] = [str(len(prt_array))+' columns in '+ks+'.'+tbl]

  # Create Workbook
  stats_sheets = {}
  worksheet = {}
  workbook = xlsxwriter.Workbook(database_url + '/' + database_name + '_' + 'astra_chart' + '.xlsx')
  
  # Create Tabs
  worksheet_metrics = workbook.add_worksheet('Astra Metrics')
  worksheet = workbook.add_worksheet('Workload')
  worksheet.freeze_panes(3,0)
  ds_worksheet = workbook.add_worksheet('Data Size')
  ds_worksheet.freeze_panes(2,2)
  for sheet_array in sheets_data:
    if (sheet_array['sheet_name'] not in exclude_tab):
      stats_sheets[sheet_array['sheet_name']] = workbook.add_worksheet(sheet_array['tab_name'])
      stats_sheets[sheet_array['sheet_name']].freeze_panes(sheet_array['freeze_row'],sheet_array['freeze_col'])
  gc_worksheet = workbook.add_worksheet('GC Pauses')
  gc_worksheet.freeze_panes(2,2)

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
      'font_size': 12,
      'border': 1,
      'font_color': 'white',
      'bg_color': '#3980D3',
      'valign': 'top'})

  header_format5 = workbook.add_format({
      'bold': True,
      'text_wrap': False,
      'font_size': 15,
      'border': 1,
      'font_color': 'white',
      'bg_color': '#3980D3',
      'valign': 'top'})

  data_format = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'valign': 'top'})

  data_format_lg = workbook.add_format({
      'text_wrap': False,
      'font_size': 13,
      'border': 1,
      'align': 'right',
      'valign': 'top'})

  data_format1 = workbook.add_format({
      'text_wrap': True,
      'font_size': 11,
      'border': 1,
      'valign': 'top'})

  data_format2 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'italic': True,
      'valign': 'top'})

  data_format3 = workbook.add_format({
      'text_wrap': True,
      'font_size': 11,
      'border': 1,
      'align': 'right',
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

  day_format1 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': 'dd \\d\\a\\y\\s hh:mm:ss',
      'valign': 'top'})

  day_format_lg = workbook.add_format({
      'text_wrap': False,
      'font_size': 13,
      'border': 1,
      'num_format': 'dd \\d\\a\\y\\s hh:mm:ss',
      'valign': 'top'})

  total_format = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '#,##0',
      'valign': 'top'})

  total_format_lg = workbook.add_format({
      'text_wrap': False,
      'font_size': 13,
      'border': 1,
      'num_format': '#,##0',
      'valign': 'top'})

  total_format1 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '[>999999]#,##0.00,," MB";[>999]0.00," KB";0',
      'valign': 'top'})

  total_format2 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '[>999999]#,##0.00,," M";[>999]0.00," K";0',
      'valign': 'top'})

  num_format_lg = workbook.add_format({
      'text_wrap': False,
      'font_size': 13,
      'border': 1,
      'num_format': '#,##0',
      'valign': 'top'})

  tps_format1 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '[>999999]#,##0.00,," (M)TPS";[>999]0.00," (K)TPS";0" TPS"',
      'valign': 'top'})

  tpmo_format1 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '[>999999]#,##0.00,," (M)TPMo";[>999]0.00," (K)TPMo";0" TPMo"',
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
      'font_size': 17,
      'border': 1,
      'align': 'center',
      'valign': 'vcenter',
      'font_color': 'white',
      'bg_color': '#3A3A42'})

  title_format4 = workbook.add_format({
      'bold': 1,
      'font_size': 14,
      'border': 1,
      'align': 'left',
      'valign': 'vcenter',
      'font_color': 'white',
      'bg_color': '#EB6C34'})

  ds_worksheet.merge_range('A1:C1', 'Table Size', title_format)

  ds_headers=['Keyspace','Table','Data Set Size']
  ds_headers_width=[14,25,17]

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

  row_num = 2
  perc_reads = 0.0
  column = 0
  total_t_size = 0
  total_set_size = 0.0
  total_row = {'read':0,'write':0,'size':0,'node':0}
  prev_nodes = []
  stat_sheets = {}
  headers = {}
  col_widths = {}
  sheets_record = {}
  node_status = 1
  proxyhistData = {}
  lpar_gr_array=[]
  lpar_tp_array=[]
  sheet_header = 0

  for node_path in os.listdir(rootPath):
    nodetoolpath = rootPath + node_path + '/nodetool'
    if path.exists(nodetoolpath):
      node = extract_ip(node_path)
      if (node==''):
        node=node_path
      if node in node_ip:
        for sheet_array in sheets_data:
          if (sheet_array['sheet_name'] not in exclude_tab):
            headers[sheet_array['sheet_name']] = sheet_array['headers']
            col_widths[sheet_array['sheet_name']] = sheet_array['widths']
            sheets_record[sheet_array['sheet_name']]={}

        for sheet_name,sheet_obj in list(stats_sheets.items()):
          if sheet_name == 'ph' and sheet_header==0:
            sheet_header=1
            sheet_obj.merge_range('A1:I1','Coordinating Node Read Latency (ms)',title_format3)
            sheet_obj.merge_range('K1:S1','Coordinating Node Write Latency (ms)',title_format3)
            row[sheet_name]=1
          elif sheet_name == 'ph':
            row[sheet_name]=1
          else:
            row[sheet_name]=0
          for col_num,header in enumerate(headers[sheet_name]):
            if header != '':
              sheet_obj.write(row[sheet_name],col_num,header,title_format)
          for col_num,col_width in enumerate(col_widths[sheet_name]):
            sheet_obj.set_column(col_num,col_num,col_width)
          row[sheet_name]+=1

        # collect dc name
        dc = ''
        info = rootPath + node_path + '/nodetool/info'
        infoFile = open(info, 'r')
        for line in infoFile:
          if('Data Center' in line):
            dc = line.split(':')[1].strip()
            try:
              type(proxyhistData[dc])
            except:
              proxyhistData[dc]={}

        # collect data from the cfstats log file
        ks = ''
        tbl = ''
        cfstat = rootPath + node_path + '/nodetool/cfstats'
        cfstatFile = open(cfstat, 'r')
        for line in cfstatFile:
          if('Keyspace' in line):
            ks = line.split(':')[1].strip()
          elif('Table: ' in line and ks not in system_keyspace):
            tbl = line.split(':')[1].strip()
          elif(':' in line and ks not in system_keyspace):
            header = line.split(':')[0].strip()
            value = line.split(':')[1].strip()
            row_data = [node,dc,ks,tbl,header,value]
            for sheet_array in sheets_data:
              if (sheet_array['sheet_name'] not in exclude_tab):
                if(sheet_array['cfstat_filter'] and sheet_array['cfstat_filter'] in line):
                  value = line.split(':')[1].strip()
                  row_data = [node,dc,ks,tbl,value]
                  if (sheet_array['filter_type']):
                    value = value.strip(sheet_array['strip'])
                    if (sheet_array['filter_type']=='>=' and float(value)>=float(sheet_array['filter'])):
                      if sheet_array['sheet_name']=='numTables' or sheet_array['sheet_name']=='partition':
                        try:
                          type(warnings['Astra Guardrails'][sheet_array['tab_name']])
                        except:
                          warnings['Astra Guardrails'][sheet_array['tab_name']]=[]
                        if(sheet_array['sheet_name']=='numTables' and len(warnings['Astra Guardrails'][sheet_array['tab_name']])==0):
                          if (float(value)>=gr_tblcnt):
                            warnings['Astra Guardrails'][sheet_array['tab_name']].append(str(value) + ' tables in database***')
                          else:
                            warnings['Astra Guardrails'][sheet_array['tab_name']].append(str(value) + ' tables in database')
                        elif sheet_array['sheet_name']=='partition':
                          table_data = dc+ks+tbl
                          if float(value)>=gr_lpar*1000000:
                            if table_data not in lpar_gr_array:
                              lpar_gr_array.append(table_data)
                              warnings['Astra Guardrails'][sheet_array['tab_name']].append('Table '+dc+'.'+ks+'.'+tbl+' partition size '+str(int(value)/1000000)+ 'MB***')
                          elif table_data not in lpar_tp_array:
                            lpar_tp_array.append(table_data)
                            warnings['Astra Guardrails'][sheet_array['tab_name']].append('Table '+dc+'.'+ks+'.'+tbl+' partition size '+str(int(value)/1000000)+ 'MB')
                          row_data[4] = str(int(value)/1000000)
                      else:
                        warnings['Database Health'][sheet_array['tab_name']]=[sheet_array['tab_name'] + ' greater than '+str(sheet_array['filter'])]
                          
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
              for record_num,record in list(sheets_record[sheet_array['sheet_name']].items()):
                new_key = sheet_array['sheet_name']+'_'+record[2]+'_'+record[3]
                if hasattr(key_record,new_key) :
                  if(key_record[new_key] < record[4]):
                    key_record[new_key] = record[4]
                    key_data[new_key] = record
                else:
                  key_record[new_key] = record[4]
                  key_data[new_key] = record

        # collect node R/W latency data - coordinator level latencies
        proxyhist = rootPath + node_path + '/nodetool/proxyhistograms'
        proxyhistFile = open(proxyhist, 'r')
        proxyhistData[dc][node] = {'Max':{},'99%':{},'98%':{},'95%':{},'75%':{},'50%':{},'Min':{}}
        for line in proxyhistFile:
          if('%' in line or 'Min' in line or 'Max' in line):
            values = line.split();
            proxyhistData[dc][node][values[0]]['R']=float(values[1])/1000
            proxyhistData[dc][node][values[0]]['W']=float(values[2])/1000
        
        for row_key in key_record:
          write_row(row_key.split('_')[0],key_data[row_key],data_format)

  # Proxyhistogram tab
  for dc,node_ph_array in list(proxyhistData.items()):
    for node,proxyhist_array in list(node_ph_array.items()):
      if node in node_ip:
        row_data = [
          dc,
          node,
          proxyhist_array['Max']['R'],
          proxyhist_array['99%']['R'],
          proxyhist_array['98%']['R'],
          proxyhist_array['95%']['R'],
          proxyhist_array['75%']['R'],
          proxyhist_array['50%']['R'],
          proxyhist_array['Min']['R'],
          '',
          dc,
          node,
          proxyhist_array['Max']['W'],
          proxyhist_array['99%']['W'],
          proxyhist_array['98%']['W'],
          proxyhist_array['95%']['W'],
          proxyhist_array['75%']['W'],
          proxyhist_array['50%']['W'],
          proxyhist_array['Min']['W'],
        ]
        write_row('ph',row_data,data_format,[9])

  # Node data tab
  ro=0
  for dc,node_status_array in list(node_status_data.items()):
    for node,status_array in list(node_status_array.items()):
      if node in node_ip:
        row_data = [dc,node,status_array['Load'],status_array['Tokens'],status_array['Rack']]
        write_row('node',row_data,data_format)
        ro = row['node']
        stats_sheets['node'].write(ro-1,5,node_uptime[node],total_format2)
        stats_sheets['node'].write_formula('G'+str(ro),'=INT(F'+str(ro)+'/86400) & " days " & TEXT((F'+str(ro)+'/86400)-INT(F'+str(ro)+'/86400),"hh:mm:ss")',data_format3)
  stats_sheets['node'].write('E'+str(ro+1),'Avg Uptime',title_format)
  stats_sheets['node'].write_formula('F'+str(ro+1),'=AVERAGE(F2:F'+str(ro)+')',total_format2)
  stats_sheets['node'].write_formula('G'+str(ro+1),'=INT(F'+str(ro+1)+'/86400) & " days " & TEXT((F'+str(ro+1)+'/86400)-INT(F'+str(ro+1)+'/86400),"hh:mm:ss")',data_format3)
  total_row['node'] = ro+1
  
  # create GC Pause tab
  gc_headers=['Name','Level/DC','Pauses','Max','P99','P98','P95','P90','P75','P50','Min','From','To','Max Date']
  gc_fields=['Name','Level','Pauses','Max','P99','P98','P95','P90','P75','P50','Min','From','To','max_gc']
  gc_widths=[18,14,8,6,6,6,6,6,6,6,6,35,35,17]

  prev_dc=0
  row_num=0
  column=0
  for header in gc_headers:
    gc_worksheet.write(row_num,column,header,title_format)
    write_cmt(worksheet,chr(ord('@')+column+1)+str(row_num+1),header)
    column+=1

  for col_num,col_width in enumerate(gc_widths):
    gc_worksheet.set_column(col_num,col_num,col_width)

  column=0
  for name, gc_val in list(gc_data.items()):
    if(gc_val['Level']=='Database'):
      row_num+=1
      for field in gc_fields:
        if(field=='From'):
          gc_worksheet.write(row_num,column,oldest_gc[name]['dt'],data_format)
        elif(field=='To'):
          gc_worksheet.write(row_num,column,newest_gc[name]['dt'])
        elif(field=='max_gc'):
          gc_worksheet.write(row_num,column,max_gc[name])
        else:
          try:
            gc_worksheet.write(row_num,column,int(gc_val[field]),num_format1)
          except:
            gc_worksheet.write(row_num,column,gc_val[field])
        column+=1
      row_num+=1
      column=0

  dc_count=0
  for name, gc_val in list(gc_data.items()):
    if(gc_val['Level']=='DC'):
      dc_count += 1
      for field in gc_fields:
        if(field=='From'):
          gc_worksheet.write(row_num,column,oldest_gc[name]['dt'])
        elif(field=='To'):
          gc_worksheet.write(row_num,column,newest_gc[name]['dt'])
        elif(field=='max_gc'):
          gc_worksheet.write(row_num,column,max_gc[name])
        elif(gc_val[field]):
          try:
            gc_worksheet.write(row_num,column,int(gc_val[field]),num_format1)
          except:
            gc_worksheet.write(row_num,column,gc_val[field])
        column+=1
      row_num+=1
      column=0

  for dc_name in dc_list:
    for name, gc_val in list(gc_data.items()):
      node_ip_addr = gc_val['Name']
      if(gc_val['Level']=='Node' and dc_name==node_dc[node_ip_addr]):
        for field in gc_fields:
          if(field=='Level'):
            gc_worksheet.write(row_num,column,node_dc[gc_val['Name']],data_format)
          elif(field=='From'):
            gc_worksheet.write(row_num,column,oldest_gc[name]['dt'],data_format)
          elif(field=='To'):
            gc_worksheet.write(row_num,column,newest_gc[name]['dt'],data_format)
          elif(field=='max_gc'):
            gc_worksheet.write(row_num,column,max_gc[name],data_format)
          elif(gc_val[field]):
            try:
              gc_worksheet.write(row_num,column,int(gc_val[field]),num_format1)
            except:
              gc_worksheet.write(row_num,column,gc_val[field],data_format)
          column+=1
        row_num+=1
        column=0
  
  # create workload tab
  wl_headers=['Keyspace','Table','Read Requests','Average TPS','% Reads','R % RW','','Keyspace','Table','Write Requests','Average TPS','% Writes','W % RW']
  wl_headers_width=[14,25,17,13,9,9,3,14,25,17,13,9,9]

  column=0
  for col_width in wl_headers_width:
    worksheet.set_column(column,column,col_width)
    column+=1

  worksheet.merge_range('A1:M1', 'Workload for '+database_name, title_format3)
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

  row_num = 2
  column = 0

  for t_data in table_count:
    ks = t_data['keyspace']
    tbl = t_data['table']
    cnt = t_data['count']
    ds_worksheet.write(row_num,column,ks,data_format)
    ds_worksheet.write(row_num,column+1,tbl,data_format)
    ds_worksheet.write(row_num,column+2,cnt,total_format1)
    row_num+=1

  total_row['size']=row_num
  ds_worksheet.write(row_num,column,'Total',header_format4)
  ds_worksheet.write(row_num,column+2,'=SUM(C3:C'+ str(row_num)+')',total_format1)

  row_num = 3
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
    table_totals[ks][tbl] = {'reads':cnt,'writes':'n/a'}
    read_subtotal += cnt
    worksheet.write(row_num,column,ks,data_format)
    worksheet.write(row_num,column+1,tbl,data_format)
    worksheet.write(row_num,column+2,cnt,total_format2)
    worksheet.write(row_num,column+3,table_tps[ks][tbl]['read'],tps_format1)
    worksheet.write(row_num,column+4,float(cnt)/total_reads,perc_format)
    worksheet.write(row_num,column+5,float(cnt)/float(total_rw),perc_format)
    row_num+=1

  total_row['read']=row_num
  
  worksheet.write(row_num,column,'Total',header_format4)
  write_cmt(worksheet,chr(ord('@')+column+1)+str(row_num+1),'Total')
  worksheet.write(row_num,column+2,'=SUM(C4:C'+ str(row_num)+')',total_format2)
  worksheet.write(row_num,column+3,'=SUM(D4:D'+ str(row_num)+')',tps_format1)
  worksheet.write(row_num,column+5,'=SUM(F4:F'+ str(row_num)+')',perc_format)
  write_cmt(worksheet,chr(ord('@')+column+6)+str(row_num+1),'Total R % RW')

  perc_writes = 0.0
  row_num = 3
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
    worksheet.write(row_num,column,ks,data_format)
    worksheet.write(row_num,column+1,tbl,data_format)
    worksheet.write(row_num,column+2,cnt,total_format2)
    worksheet.write(row_num,column+3,table_tps[ks][tbl]['write'],tps_format1)
    worksheet.write(row_num,column+4,float(cnt)/total_writes,perc_format)
    worksheet.write(row_num,column+5,float(cnt)/float(total_rw),perc_format)
    row_num+=1

  total_row['write']=row_num

  worksheet.write(row_num,column,'Total',header_format4)
  write_cmt(worksheet,chr(ord('@')+column+1)+str(row_num+1),'Total')
  worksheet.write(row_num,column+2,'=SUM(J4:J'+ str(row_num)+')',total_format2)
  worksheet.write(row_num,column+3,'=SUM(K4:K'+ str(row_num)+')',tps_format1)
  worksheet.write(row_num,column+5,'=SUM(M4:M'+ str(row_num)+')',perc_format)
  write_cmt(worksheet,chr(ord('@')+column+6)+str(row_num+1),'Total W % RW')

  # create the Astra Metrics tab
  worksheet_metrics.set_column(0,0,30)
  worksheet_metrics.set_column(1,1,40)

  row_num=2
  column=0
  worksheet_metrics.merge_range('A1:B1', 'Astra Metrics Data for '+database_name, title_format3)
  worksheet_metrics.merge_range('A2:B2', 'Workload Summary', header_format5)
  worksheet_metrics.write(row_num,column,'Read TPS',title_format4)
  write_cmt(worksheet_metrics,chr(ord('@')+column+1)+str(row_num+1),'Read TPS')
  worksheet_metrics.write_formula('B'+str(row_num+1),'=Workload!D'+str(total_row['read']+1),num_format_lg)
  worksheet_metrics.write(row_num+1,column,'Read TPMo',title_format4)
  write_cmt(worksheet_metrics,chr(ord('@')+column+1)+str(row_num+2),'Read TPMo')
  worksheet_metrics.write_formula('B'+str(row_num+2),'=Workload!D'+str(total_row['read']+1)+'*60*60*24*365.25/12',num_format_lg)
  worksheet_metrics.write(row_num+2,column,'Write TPS',title_format4)
  write_cmt(worksheet_metrics,chr(ord('@')+column+1)+str(row_num+3),'Write TPS')
  worksheet_metrics.write_formula('B'+str(row_num+3),'=Workload!K'+str(total_row['write']+1),num_format_lg)
  worksheet_metrics.write(row_num+3,column,'Write TPMo',title_format4)
  write_cmt(worksheet_metrics,chr(ord('@')+column+1)+str(row_num+4),'Write TPMo')
  worksheet_metrics.write_formula('B'+str(row_num+4),'=Workload!K'+str(total_row['write']+1)+'*60*60*24*365.25/12',num_format_lg)
  worksheet_metrics.write(row_num+4,column,'Data Size (GB)',title_format4)
  write_cmt(worksheet_metrics,chr(ord('@')+column+1)+str(row_num+5),'Data Size (GB)')
  worksheet_metrics.write_formula('B'+str(row_num+5),"='Data Size'!C"+str(total_row['size']+1)+'/1000000000',num_format_lg)
  worksheet_metrics.write(row_num+5,column,'Average Uptime',title_format4)
  write_cmt(worksheet_metrics,chr(ord('@')+column+1)+str(row_num+5),'Average Uptime')
  worksheet_metrics.write_formula('B'+str(row_num+6),"='Node Data'!G"+str(total_row['node']),data_format_lg)

  row_num+=7
  start_row=row_num
  for warn_cat_title,warn_cat_array in list(warnings.items()):
    row_num+=1
    worksheet_metrics.merge_range('A'+str(row_num)+':B'+str(row_num),warn_cat_title,header_format5)
    row_num+=1
    for warn_title,warn_array in list(warn_cat_array.items()):
      if (len(warn_array)):
        worksheet_metrics.merge_range('A'+str(row_num)+':B'+str(row_num),warn_title,title_format4)
        write_cmt(worksheet_metrics,chr(ord('@')+column+1)+str(row_num),warn_title)
        row_num+=1
        for warn in warn_array:
          worksheet_metrics.merge_range('A'+str(row_num)+':B'+str(row_num),warn,data_format1)
          row_num+=1
  if (row_num==start_row):
    worksheet_metrics.merge_range('A'+str(row_num+1)+':B'+str(row_num+1),'No potential guardrail issues identified',data_format1)
    row_num+=2

  worksheet_metrics.insert_textbox('D2',info_box,info_box_options)

  worksheet_metrics.activate()
  workbook.close()
  print((('"' + database_name + '_' + 'astra_chart' + '.xlsx"' + ' was created in "' + database_url) +'"'))
exit();

