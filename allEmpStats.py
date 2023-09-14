# -*- coding: utf-8 -*-
"""
Created on Thu May 11 14:15:47 2023

@author: shank
"""

import json
import pandas as pd
import datetime
import time
import requests
import pytz
from tqdm import tqdm
import sys

def convert_milliseconds_to_hours_minutes(milliseconds):
    seconds = milliseconds / 1000
    minutes = seconds // 60
    hours = minutes // 60
    minutes = minutes % 60
    return (str(int(hours)) + ' h ' + str(int(minutes)) + ' m')
    # return (int(hours), int(minutes))
    
def unixTimeToText(unixTime):
    if unixTime is not None:
        unixTime = int(unixTime)//1000
        unixTime = datetime.datetime.fromtimestamp(unixTime, tz=pytz.utc).astimezone(ist_tz)
        return unixTime.strftime('%m/%d/%Y, %I:%M:%S %p IST')
    else:
        return ''

if len(sys.argv) != 3:
    print("Usage: python allEmpStats.py start_date end_date")
    sys.exit(1)
    
start = time.time()
QUOTA = 0

try:
    start_date = int(sys.argv[1])
    end_date = int(sys.argv[2])
except ValueError:
    print("Invalid date format. Dates should be in Unix timestamp format.")
    sys.exit(1)
    
# start_date = 1672531200
# end_date = 1685577599

team_id = "3314662"
headers = {
  "Content-Type": "application/json",
  "Authorization": "pk_3326657_EOM3G6Z3CKH2W61H8NOL5T7AGO9D7LNN"
}

# Extract spaceID vs SpaceName dictionary
url = "https://api.clickup.com/api/v2/team/" + team_id + "/space"
query = {
  "archived": "false"
}
response = requests.get(url, headers=headers, params=query)
data = response.json()
# Extract 'id' and 'name' into a separate dictionary
spaces = {item['id']: item['name'] for item in data['spaces']}

# # Load the JSON file
# with open('teamInfo.json', 'r') as f:
#     data = json.load(f)

# Extract Team members info
url = "https://api.clickup.com/api/v2/team"
headers = {
  "Content-Type": "application/json",
  "Authorization": "pk_3326657_EOM3G6Z3CKH2W61H8NOL5T7AGO9D7LNN"
}
response = requests.get(url, headers=headers)
data = response.json()
team = data['teams'][0]

# Write the dictionary to the JSON file
with open('teamInfo.json', "w") as json_file:
    json.dump(data, json_file)
    
# Extract member id and username
members_dict = {}
# for team in data['teams']:
for member in team['members']:
    member_id = member['user']['id']
    member_username = member['user']['username']
    members_dict[member_id] = member_username

count = 0
num_of_err = 0

# Initialize empty lists for each column
emp_names = []
emp_ids = []
task_names = []
task_ids = []
task_status = []
start_times = []
end_times = []
durations = []
durations_hh = []
space_ids = []
folder_ids = []
list_ids = []
total_time_spent_till_date = []
descriptions = []
billable = []
tags = []
time_entry_ids =[]

# Loop over every employee
for key in members_dict:#[49207289, 3426506]:
    # If the Name is NoneType skip the iteration
    if not members_dict[key]:
        continue
    
    # Extract time entries of an employee as JSON    
    url = "https://api.clickup.com/api/v2/team/" + team_id + "/time_entries"
    query = {
      "start_date": str(int(start_date*1000) - 19800000),
      "end_date": str(int(end_date*1000) - 19800000),
      "assignee": key,
    }
    
    response = requests.get(url, headers=headers, params=query)
    QUOTA += 1
    data = response.json()    
    
    
    # Loop through the data and extract the required fields
    for entry in data['data']:
        try:
            emp_names.append(entry['user']['username'])
            emp_ids.append(entry['user']['id'])
            task_names.append(entry['task']['name'])
            task_ids.append(entry['task']['id'])
            task_status.append(entry['task']['status']['status'])            
            space_ids.append(entry['task_location']['space_id'])
            folder_ids.append(entry['task_location']['folder_id'])
            list_ids.append(entry['task_location']['list_id'])
            descriptions.append(entry['description'])
            billable.append(entry['billable'])
            tags.append(entry['tags'])
            time_entry_ids.append(entry['id'])
        except:
            task_names.append('0')
            task_ids.append('0')
            task_status.append('0')
            space_ids.append('0')
            folder_ids.append('0')
            list_ids.append('0')
            descriptions.append('0')
            billable.append('0')
            tags.append('0')
            time_entry_ids.append('0')
            
        durations.append(int(entry['duration']))
        durations_hh.append(convert_milliseconds_to_hours_minutes(int(entry['duration'])))
        start_times.append(int(entry['start'])//1000) # Convert to seconds        
        end_times.append(int(entry['end'])//1000)        
    
    print(members_dict[int(key)]+' data extracted')        

# Define the IST timezone
ist_tz = pytz.timezone('Asia/Kolkata')
# Convert the UTC start time to IST
start_time_ist = [datetime.datetime.fromtimestamp(tt, tz=pytz.utc).astimezone(ist_tz) for tt in start_times]
end_time_ist = [datetime.datetime.fromtimestamp(tt, tz=pytz.utc).astimezone(ist_tz) for tt in end_times]
# Format the IST start time as "DD Mon YYYY, HH:MM:SS"
start_times_readable = [tt.strftime('%m/%d/%Y, %I:%M:%S %p IST') for tt in start_time_ist]
end_times_readable = [tt.strftime('%m/%d/%Y, %I:%M:%S %p IST') for tt in end_time_ist]
# start_times_readable = [tt.strftime('%d %b %Y, %H:%M:%S') for tt in start_time_ist]
# end_times_readable = [tt.strftime('%d %b %Y, %H:%M:%S') for tt in end_time_ist]

# Create a pandas dataframe
df = pd.DataFrame({
    'Emp Name': emp_names,
    'Emp ID': emp_ids,
    'Task Name': task_names,
    'Task ID': task_ids,
    'Task Status': task_status,
    'Start': start_times,
    'Start Text': start_times_readable,
    'Stop': end_times,
    'Stop Text': end_times_readable,
    'Time Tracked': durations,
    'Time Tracked Text': durations_hh,
    'Space ID': space_ids,
    'Folder ID': folder_ids,
    'List ID': list_ids,    
    'Description': descriptions,
    'Billable': billable,
    'Tags': tags,
    'Time Entry ID': time_entry_ids
    # 'Day': days
})    

# Create a new DataFrame with only unique Task IDs
task_ids = df['Task ID'].unique()
new_df = pd.DataFrame({'Task ID': task_ids})


# iterate over the unique task IDs in the dataframe
num_of_err = 0
uniqueTaskList = df['Task ID'].unique()

# Wrap the loop with tqdm
# for item in tqdm(data, total=len(data)):
for task_id in tqdm(uniqueTaskList, total=len(uniqueTaskList)):    
    count += 1
    # construct the API URL for the task ID
    url = "https://api.clickup.com/api/v2/task/" + task_id

    # make the API request and parse the JSON response
    response = requests.get(url, headers=headers)
    QUOTA += 1
    # if QUOTA > 98:
    #     QUOTA = 0
    #     print('API Limit reached, Program will pause for a minute: Errors = ', num_of_err)
    #     time.sleep(61)
    #     response = requests.get(url, headers=headers)
        
    tasks = response.json()
    if 'err' in tasks:        
        num_of_err += 1
        print('\nError: ', tasks['err'], 'No. of tasks processed: ', count)
        print('Program will pause for a {}: Errors = {}'.format(60, num_of_err))
        print('Task ID = ', task_id)
        time.sleep(60)
        if tasks['err'] == 'Rate limit reached':
            response = requests.get(url, headers=headers)
            tasks = response.json()
        else:
            continue
    
    df.loc[df['Task ID'] == task_id, 'Task Time Spent'] = tasks['time_spent']
    df.loc[df['Task ID'] == task_id, 'Task Time Spent Text'] = convert_milliseconds_to_hours_minutes(tasks['time_spent'])
    df.loc[df['Task ID'] == task_id, 'Checklists'] = str(tasks['checklists'])
    df.loc[df['Task ID'] == task_id, 'Date Created'] = tasks['date_created']
    df.loc[df['Task ID'] == task_id, 'Date Created Text'] = unixTimeToText(tasks['date_created'])
    df.loc[df['Task ID'] == task_id, 'Due Date'] = tasks['due_date']
    df.loc[df['Task ID'] == task_id, 'Due Date Text'] = unixTimeToText(tasks['due_date'])
    df.loc[df['Task ID'] == task_id, 'Space Name'] = tasks['list']['name']
    df.loc[df['Task ID'] == task_id, 'Folder Name'] = tasks['folder']['name']
    df.loc[df['Task ID'] == task_id, 'List Name'] = tasks['list']['name']
    df.loc[df['Task ID'] == task_id, 'Start Date'] = tasks['start_date']
    df.loc[df['Task ID'] == task_id, 'Start Date Text'] = unixTimeToText(tasks['start_date'])
    df.loc[df['Task ID'] == task_id, 'Task Time Estimated'] = tasks['time_estimate']
    df.loc[df['Task ID'] == task_id, 'Task Time Estimated Text'] = unixTimeToText(tasks['time_estimate'])
    # If there is no Custom field just continue
    try:
        # iterate over the custom fields for the task
        for custom_field in tasks['custom_fields']:
            if 'value' in custom_field:
                if custom_field['type'] == 'drop_down':
                    # set the value in the dataframe for the current task ID and custom field name
                    df.loc[df['Task ID'] == task_id, custom_field['name']] = custom_field['type_config']['options'][custom_field['value']]['name']
    except:
       pass                    

# Create 'Space Name' column based on 'Space ID'
df['Space Name'] = df['Space ID'].map(spaces)

df.to_csv('allEmployeeClickUp.csv', index=False)   
print(time.time()-start)        
        
         