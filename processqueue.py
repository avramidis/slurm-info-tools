# @authors: Eleftherios Avramidis
# @email: el.avramidis@gmail.com
# @date: 2018/02/23
# @copyright: MIT License

import subprocess
import pandas
import datetime
import numpy

def get_slurm_job_data(starttime, endtime):
 
  # Check that the date difference if not larger that 4 days
  datetime_submit_time = datetime.datetime.strptime(starttime, '%Y-%m-%d')
  datetime_start_time = datetime.datetime.strptime(endtime, '%Y-%m-%d')
  
  
  if (datetime_start_time-datetime_submit_time).total_seconds()> 345600:
    print("Warning selected time period is larger than 96 hours (4 days)!")
  
  # Run the sacct command to get the data from the cluster database
  subprocess.call(' '.join(["sacct -a --starttime ", starttime, " --endtime ", endtime, " --state COMPLETED --partition skylake --format=JobID%30,Submit,start,Account%30,ReqNodes,ReqCPUS | sed -n '2!p' > jobsinfo.txt"]), shell=True)
  
  # Initialise dataframes
  # Jobs info dataframe
  df_jobsinfo = pandas.read_csv('jobsinfo.txt', skiprows=[1], delim_whitespace=True )
  
  # Results dataframe
  df_all = pandas.DataFrame(columns=df_jobsinfo.columns.values)
  df_waittimes = pandas.DataFrame(columns=['WAIT(s)'])
  
  # Iterates the rows in df_jobsinfo to check it the job is in df_all
  for index, row in df_jobsinfo.iterrows():
    res = df_all.loc[df_all['JobID']==row['JobID']]
    
    df_contains_dot = row['JobID'].find(".")
    
    if ((res.empty) and (df_contains_dot<0)):
      row['Submit'] = str(row['Submit']).replace('T', ' ')
      row['Start'] = str(row['Start']).replace('T', ' ')
    
      df_all = df_all.append(row, ignore_index=True)
      
      datetime_submit_time = datetime.datetime.strptime(row['Submit'], '%Y-%m-%d %H:%M:%S')
      datetime_start_time = datetime.datetime.strptime(row['Start'], '%Y-%m-%d %H:%M:%S')
      
      # Add the calculated wait time   
      df_new_data = pandas.DataFrame([(datetime_start_time-datetime_submit_time).total_seconds()], columns=['WAIT(s)'])   
      df_waittimes = df_waittimes.append(df_new_data)
  
  df_all=df_all.reset_index(drop=True)
  df_waittimes=df_waittimes.reset_index(drop=True)
  df_all = pandas.concat([df_all, df_waittimes['WAIT(s)']], axis=1)
  
  df_sl1 = df_all[df_all.Account.str.contains('sl1')]
  df_sl2 = df_all[df_all.Account.str.contains('sl2')]
  df_sl3 = df_all[df_all.Account.str.contains('sl3')]
  df_sl4 = df_all[df_all.Account.str.contains('sl4')]
  
  df_all.to_csv('results_all.txt', sep='\t')
  df_sl1.to_csv('results_sl1.txt', sep='\t')
  df_sl2.to_csv('results_sl2.txt', sep='\t')
  df_sl3.to_csv('results_sl3.txt', sep='\t')
  df_sl4.to_csv('results_sl4.txt', sep='\t')
  
  
  ##########################################################
  ## Results array
  results = numpy.zeros((4,3))
  
  ##########################################################
  ## Calculate mean wait time per account type
  temp = df_sl1['WAIT(s)']
  results[0][0]=temp.mean()
  print("SL1 mean wait time: ", results[0][0])
  
  temp = df_sl2['WAIT(s)']
  results[1][0]=temp.mean()
  print("SL2 mean wait time: ", results[1][0])
  
  temp = df_sl3['WAIT(s)']
  results[1][0]=temp.mean()
  print("SL3 mean wait time: ", results[2][0])
  
  temp = df_sl4['WAIT(s)']
  results[1][0]=temp.mean()
  print("SL4 mean wait time: ", results[3][0])
  
  print("")
  
  ##########################################################
  ## Calculate max wait time per account type
  temp = df_sl1['WAIT(s)']
  results[0][1]=temp.max()
  print("SL1 max wait time: ", results[0][1])
  
  temp = df_sl2['WAIT(s)']
  results[1][1]=temp.max()
  print("SL2 max wait time: ", results[1][1])
  
  temp = df_sl3['WAIT(s)']
  results[2][1]=temp.max()
  print("SL3 max wait time: ", results[2][1])
  
  temp = df_sl4['WAIT(s)']
  results[3][1]=temp.max()
  print("SL4 max wait time: ", results[3][1])
  
  print("")
  
  ##########################################################
  ## Calculate min wait time per account type
  temp = df_sl1['WAIT(s)']
  results[0][2]=temp.min()
  print("SL1 min wait time: ", results[0][2])
  
  temp = df_sl2['WAIT(s)']
  results[1][2]=temp.min()
  print("SL2 min wait time: ", results[1][2])
  
  temp = df_sl3['WAIT(s)']
  results[2][2]=temp.min()
  print("SL3 min wait time: ", results[2][2])
  
  temp = df_sl4['WAIT(s)']
  results[3][2]=temp.min()
  print("SL4 min wait time: ", results[3][2])
  
  return results
  
def get_min_per_day(starttime, endtime):
  
  submit_time = datetime.datetime.strptime(starttime, '%Y-%m-%d')
  endtime_time = datetime.datetime.strptime(endtime, '%Y-%m-%d')

  results = get_slurm_job_data(submit_time, endtime_time)
  
  print(results)

  
if __name__ == '__main__':
  
  starttime='2018-04-28'
  endtime='2018-04-29'
  
  datetime_submit_time = datetime.datetime.strptime(starttime, '%Y-%m-%d')
  datetime_submit_time = datetime.datetime.strptime(starttime, '%Y-%m-%d')

  results = get_slurm_job_data(starttime, endtime)
  
  print(results)
  
  #mins = numpy.zeros(5)
  
  