# -*- coding: utf-8 -*-
"""
Created on Thu May 11 14:15:47 2023

@author: shank
"""

import json
import argparse
import getpass
import re
import pandas as pd
import datetime
import time
import requests
import pytz
from tqdm import tqdm
import sys
import os
import hashlib
import tempfile
import shutil
import fcntl
from pathlib import Path

def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(
        description='Export ClickUp time entries. Calendar dates use Asia/Kolkata (IST).',
        epilog='Examples: python allEmpStats.py 2026-09-01 2026-09-30 --entries-only; '
               'python allEmpStats.py 1788220800 1790812799 --entries-only')
    parser.add_argument('start', help='First day YYYY-MM-DD (IST), or Unix timestamp in seconds')
    parser.add_argument('end', help='Last day YYYY-MM-DD (IST, inclusive), or Unix timestamp in seconds (inclusive)')
    parser.add_argument('--workspace-id', default='3314662', help='ClickUp workspace ID (default: %(default)s)')
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument('--roster', type=Path, help='Historical employee JSON; current members are also included')
    selection.add_argument('--current-only', action='store_true', help='Use current workspace members only')
    parser.add_argument('--entries-only', action='store_true', help='Export raw entries and coverage without task enrichment')
    parser.add_argument('--fresh', action='store_true', help='Discard checkpoints for this run and fetch fresh responses')
    args = parser.parse_args(argv)
    tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    def timestamp(value, end=False):
        if re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value):
            day = datetime.datetime.strptime(value, '%Y-%m-%d').replace(tzinfo=tz)
            return int((day + datetime.timedelta(days=1)).timestamp()) - 1 if end else int(day.timestamp())
        if not re.fullmatch(r'[0-9]{1,11}', value):
            raise ValueError('Use YYYY-MM-DD or a Unix timestamp in seconds, not milliseconds.')
        result = int(value)
        datetime.datetime.fromtimestamp(result, tz)
        return result
    try:
        args.start_seconds = timestamp(args.start)
        args.end_seconds = timestamp(args.end, end=True)
    except (ValueError, OverflowError, OSError) as exc:
        parser.error(str(exc))
    if args.start_seconds > args.end_seconds:
        parser.error('Start must be on or before end.')
    first = datetime.datetime.fromtimestamp(args.start_seconds, tz)
    last = datetime.datetime.fromtimestamp(args.end_seconds, tz)
    if first.time() == datetime.time(0, 0) and last.time() == datetime.time(23, 59, 59):
        args.period_label = first.strftime('%Y-%m-%d') + '_to_' + last.strftime('%Y-%m-%d')
    else:
        args.period_label = first.strftime('%Y%m%dT%H%M%S') + '_to_' + last.strftime('%Y%m%dT%H%M%S')
    print(f'Report period (IST): {first.isoformat()} through {last.isoformat()} inclusive')
    return args


def find_employee_roster(args, script_dir):
    if args.current_only:
        return None
    if args.roster is not None:
        path = args.roster.expanduser().resolve()
        if not path.is_file():
            raise SystemExit(f'Roster file not found: {path}. Copy it to this computer or correct --roster. '
                             'Use --current-only only if you want to exclude historical-only accounts.')
        return path
    path = script_dir / 'allEmpStats-roster.json'
    if path.is_file():
        return path
    candidates = sorted(script_dir.glob('allEmpStats-roster-*.json'))
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        raise SystemExit('Multiple historical rosters found. Select one explicitly with --roster PATH.')
    raise SystemExit('Historical employee roster missing. Put allEmpStats-roster.json beside this script '
                     'or specify --roster PATH. To deliberately use current members only, add --current-only.')


def load_employee_roster(path, workspace_id):
    if path is None:
        return {'workspace_id': str(workspace_id), 'employees': []}
    try:
        with path.open(encoding='utf-8') as source:
            roster = json.load(source)
        if str(roster.get('workspace_id')) != str(workspace_id):
            raise ValueError('Roster belongs to a different workspace')
        if not isinstance(roster.get('employees'), list):
            raise ValueError('Expected an employees list')
        for employee in roster['employees']:
            if not employee.get('employee_code') or not employee.get('name') or not isinstance(employee.get('clickup_ids'), list):
                raise ValueError('Each employee needs employee_code, name and clickup_ids')
            if any(not str(uid).isdigit() for uid in employee['clickup_ids']):
                raise ValueError(f"Invalid ClickUp user ID for {employee['employee_code']}")
    except (OSError, ValueError, TypeError, KeyError) as exc:
        raise SystemExit(f'Cannot load employee roster {path}: {exc}') from exc
    print(f'Historical roster: {path.name} (source period: {roster.get("date_range", "unspecified")}). '
          'Its known IDs are included alongside current members; it does not limit the report dates.')
    return roster


def merge_employee_roster(roster, team):
    members, codes = {}, {}
    for member in team.get('members', []):
        user = member.get('user') or {}
        if not str(user.get('id', '')).isdigit():
            continue
        uid = int(user['id'])
        members[uid] = user.get('username') or user.get('email') or f'User {uid}'
        codes[uid] = re.findall(r'(?<![A-Za-z0-9])[A-Z][0-9]{3}(?![A-Za-z0-9])', members[uid])
    unresolved = []
    for employee in roster['employees']:
        ids = employee['clickup_ids'] or [uid for uid, values in codes.items() if employee['employee_code'] in values]
        if not ids:
            unresolved.append(employee)
        for uid in ids:
            uid = int(uid)
            members.setdefault(uid, employee['name'])
            codes.setdefault(uid, []).append(employee['employee_code'])
    if not members:
        raise SystemExit('No ClickUp user IDs are available for this report.')
    return members, {uid: sorted(set(values)) for uid, values in codes.items()}, unresolved


def checkpoint_key(url, params):
    request = json.dumps({'url': url, 'params': params}, sort_keys=True)
    return hashlib.sha256(request.encode('utf-8')).hexdigest()


def save_checkpoint(path, data):
    # Replace only after a complete write; interruption leaves the old file intact.
    fd, temporary = tempfile.mkstemp(prefix='.pending-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as output:
            json.dump(data, output)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def clickup_get(url, headers, params=None, expected_key=None):
    """Validate ClickUp responses before using report data."""
    cache_path = checkpoint_dir / (checkpoint_key(url, params) + '.json')
    if cache_path.exists():
        try:
            with cache_path.open(encoding='utf-8') as saved:
                data = json.load(saved)
            if not isinstance(data, dict) or 'err' in data:
                raise ValueError('Invalid saved response')
            if expected_key is not None and not isinstance(data.get(expected_key), list):
                raise ValueError('Invalid saved response shape')
            return data
        except (ValueError, OSError) as exc:
            raise SystemExit(
                f"Cannot read checkpoint {cache_path}: {exc}. "
                "Rerun with --fresh to discard this run's saved progress."
            ) from exc
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
    except requests.RequestException as exc:
        raise SystemExit(f"ClickUp request failed: {exc}") from exc
    try:
        data = response.json()
    except ValueError as exc:
        raise SystemExit(
            f"ClickUp returned HTTP {response.status_code} with a non-JSON response."
        ) from exc
    if not response.ok or (isinstance(data, dict) and 'err' in data):
        error = data.get('err', 'Request failed') if isinstance(data, dict) else 'Request failed'
        code = data.get('ECODE', 'unknown') if isinstance(data, dict) else 'unknown'
        message = f"ClickUp API error (HTTP {response.status_code}, {code}): {error}"
        if response.status_code == 401 or code in ('OAUTH_019', 'OAUTH_021', 'OAUTH_025', 'OAUTH_077'):
            message += " Update the Authorization token in this script with a valid ClickUp personal API token."
        raise SystemExit(message)
    if expected_key is not None and (
        not isinstance(data, dict) or not isinstance(data.get(expected_key), list)
    ):
        raise SystemExit(f"Unexpected ClickUp response: expected a '{expected_key}' list.")
    if not isinstance(data, dict):
        raise SystemExit('Unexpected ClickUp response: expected a JSON object.')
    save_checkpoint(cache_path, data)
    return data


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

args = parse_arguments()
start = time.time()
QUOTA = 0
start_date = args.start_seconds
end_date = args.end_seconds
date_range = args.period_label

# Define the list of columns to check for NaN
columns_to_check = [
    "Course",
    "Product",
    "Common Activities",
    "Outside Office Tasks",
    "Management Activities",
    "Technology",
    "Linguistic-Project",
    "Multi-Media",
    "Customer Seva",
    "Sales & Marketing",
    "ELearning",
    "Kids Persona",
    "Finance",
    "Website",
    "SFH Admin",
    "Admin",
    "Linguistic Activity",
    "HR & Admin",
    "Proj-Common-Activity",
    "Proj-Outside-Office",
    "Management-Project",
    "Technology-Project",
    "MMedia-Project",
    "Project-CST",
    "Sales-Mktg-Project",
    "Project-ELA",
    "Proj-KidsPersona",
    "FinAcc-Project",
    "SFH-Admin-Project",
    "Admin-Project",
    "Linguistic-Activity"
]

team_id = args.workspace_id
api_token = os.environ.get('CLICKUP_API_TOKEN', '').strip()
if not api_token:
    if sys.stdin.isatty():
        api_token = getpass.getpass('ClickUp admin API token: ').strip()
    else:
        raise SystemExit('Set CLICKUP_API_TOKEN to your ClickUp admin API token before running this script.')
if not api_token:
    raise SystemExit('A ClickUp API token is required.')
headers = {'Content-Type': 'application/json', 'Authorization': api_token}

roster_path = find_employee_roster(args, Path(__file__).resolve().parent)
roster = load_employee_roster(roster_path, team_id)

# Checkpoints live beside the script and contain successful API responses only.
# Credentials are never written to checkpoint files.
roster_identity = hashlib.sha256(json.dumps(roster, sort_keys=True).encode('utf-8')).hexdigest()
run_identity = json.dumps([3, str(team_id), start_date, end_date, roster_identity])
run_key = hashlib.sha256(run_identity.encode('utf-8')).hexdigest()[:20]
checkpoint_root = Path(__file__).resolve().parent / '.allEmpStats-checkpoints'
checkpoint_root.mkdir(exist_ok=True)
# Prevent two copies of the same run from modifying the same checkpoint files.
run_lock = (checkpoint_root / (run_key + '.lock')).open('a')
try:
    fcntl.flock(run_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    run_lock.close()
    raise SystemExit('This report is already running in another process.')
try:
    checkpoint_dir = checkpoint_root / run_key
    if args.fresh and checkpoint_dir.exists():
        shutil.rmtree(checkpoint_dir)
    checkpoint_dir.mkdir(exist_ok=True)
    saved_count = len(list(checkpoint_dir.glob('*.json')))
    if saved_count:
        print(f'Resuming report using {saved_count} saved API responses.')
    else:
        print('Starting report; progress will be saved after each API response.')
    print(f'Checkpoint folder: {checkpoint_dir}')

    # Query current members as well as the historical IDs, for any selected period.
    workspace_data = clickup_get('https://api.clickup.com/api/v2/team', headers, expected_key='teams')
    team = next((item for item in workspace_data['teams'] if str(item['id']) == str(team_id)), None)
    if team is None:
        raise SystemExit(f'Workspace {team_id} is not accessible with this token.')
    members_dict, employee_codes, unresolved_employees = merge_employee_roster(roster, team)
    coverage = [{'Employee Code(s)': employee['employee_code'], 'Employee Name': employee['name'],
                 'ClickUp User ID': '', 'Status': 'Unresolved ClickUp user ID', 'Time Entries': None}
                for employee in unresolved_employees]
    unresolved_count = len(coverage)
    print(f'Employee selection: {len(members_dict)} current/historical ClickUp accounts; '
          f'{unresolved_count} employee codes unresolved.')

    # Extract spaceID vs SpaceName dictionary
    url = "https://api.clickup.com/api/v2/team/" + team_id + "/space"
    query = {
      "archived": "false"
    }
    data = clickup_get(url, headers, params=query, expected_key='spaces')
    # Extract 'id' and 'name' into a separate dictionary
    spaces = {item['id']: item['name'] for item in data['spaces']}

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
    time_entry_user = []
    goal_type = []

    # Loop over every employee
    for key in members_dict:  #[49207289]: #members_dict
        # If the Name is NoneType skip the iteration
        if not members_dict[key]:
            continue

        # Extract time entries of an employee as JSON
        url = "https://api.clickup.com/api/v2/team/" + team_id + "/time_entries"
        query = {
          "start_date": str(int(start_date*1000)),
          "end_date": str(int(end_date*1000) + 999),
          "assignee": key,
        }

        data = clickup_get(url, headers, params=query, expected_key='data')
        QUOTA += 1


        coverage.append({'Employee Code(s)': ', '.join(sorted(set(employee_codes[key]))),
                         'Employee Name': members_dict[key], 'ClickUp User ID': key,
                         'Status': 'Retrieved' if data['data'] else 'No entries returned',
                         'Time Entries': len(data['data'])})

        # Loop through the data and extract the required fields
        for entry in data['data']:
            user = entry.get('user') or {}
            task = entry.get('task') or {}
            location = entry.get('task_location') or {}
            emp_names.append(user.get('username') or members_dict[key])
            emp_ids.append(user.get('id') or key)
            task_names.append(task.get('name', '0'))
            task_ids.append(str(task.get('id') or '0'))
            task_status.append((task.get('status') or {}).get('status', '0'))
            space_ids.append(location.get('space_id', '0'))
            folder_ids.append(location.get('folder_id', '0'))
            list_ids.append(location.get('list_id', '0'))
            descriptions.append(entry.get('description', ''))
            billable.append(entry.get('billable', False))
            tags.append(entry.get('tags', []))
            time_entry_ids.append(entry.get('id', '0'))
            time_entry_user.append(user.get('username') or members_dict[key])

            durations.append(int(entry['duration']))
            durations_hh.append(convert_milliseconds_to_hours_minutes(int(entry['duration'])))
            start_times.append(int(entry['start'])//1000) # Convert to seconds
            end_times.append(int(entry['end'])//1000 if entry.get('end') is not None else None)

        print(members_dict[int(key)]+' data extracted')

    # Define the IST timezone
    ist_tz = pytz.timezone('Asia/Kolkata')
    # Convert the UTC start time to IST
    start_time_ist = [datetime.datetime.fromtimestamp(tt, tz=pytz.utc).astimezone(ist_tz) for tt in start_times]
    end_time_ist = [datetime.datetime.fromtimestamp(tt, tz=pytz.utc).astimezone(ist_tz) if tt is not None else None for tt in end_times]
    # Format the IST start time as "DD Mon YYYY, HH:MM:SS"
    start_times_readable = [tt.strftime('%m/%d/%Y, %I:%M:%S %p IST') for tt in start_time_ist]
    end_times_readable = [tt.strftime('%m/%d/%Y, %I:%M:%S %p IST') if tt is not None else '' for tt in end_time_ist]
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
        'Time Entry ID': time_entry_ids,
        'Time Entry User': time_entry_user
        # 'Goal Type': goal_type
    })

    # Save all returned entries before applying any business classification filters.
    # Multiple codes can belong to the same account after a promotion/code change.
    df['Employee Code(s)'] = df['Emp ID'].map(
        {uid: ', '.join(sorted(set(codes))) for uid, codes in employee_codes.items()}
    )
    df['Space Name'] = df['Space ID'].map(spaces).astype('object')
    df['Task URL'] = df['Task ID'].map(lambda task_id: f'https://app.clickup.com/t/{task_id}' if task_id != '0' else '')
    entries_filename = f'allEmployeeClickUp{date_range}_allEntries.csv'
    coverage_filename = f'allEmployeeClickUp{date_range}_employeeCoverage.csv'
    df.to_csv(entries_filename, index=False)
    coverage_df = pd.DataFrame(coverage)
    coverage_df['Time Entries'] = coverage_df['Time Entries'].astype('Int64')
    coverage_df.to_csv(coverage_filename, index=False)
    print(f'Saved {len(df)} entries and employee coverage. Unresolved employee codes: {unresolved_count}.')
    if args.entries_only:
        print('Time-entry extraction complete. Checkpoints retained for a subsequent full report run.')
        sys.exit(0)

    # Create a new DataFrame with only unique Task IDs
    task_ids = df['Task ID'].unique()
    new_df = pd.DataFrame({'Task ID': task_ids})


    # iterate over the unique task IDs in the dataframe
    num_of_err = 0
    uniqueTaskList = df['Task ID'].unique()

    # Wrap the loop with tqdm
    # for item in tqdm(data, total=len(data)):
    for task_id in tqdm(uniqueTaskList):#, total=len(uniqueTaskList)):
        if str(task_id) == '0':
            continue
        count += 1
        # construct the API URL for the task ID
        url = "https://api.clickup.com/api/v2/task/" + task_id

        # Cached successful tasks are reused; failed requests stop without losing progress.
        tasks = clickup_get(url, headers)
        QUOTA += 1

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
        df.loc[df['Task ID'] == task_id, 'Assignees'] = ', '.join([assignee['username'] for assignee in tasks['assignees']])
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
    df['Space Name'] = df['Space ID'].map(spaces).astype('object')
    df['Task URL'] = 'https://app.clickup.com/t/' + df['Task ID']


    ### Check if Goal Type and Project fields are set ####
    # Check for the string 'nan' in 'Goal Type' column and create a new DataFrame with the corresponding values
    if 'Goal Type' not in df:
        df['Goal Type'] = pd.Series(index=df.index, dtype='object')
    missing_goal = df['Goal Type'].isna() | df['Goal Type'].astype(str).str.strip().str.lower().eq('nan')
    nan_rows = df[missing_goal][['Emp Name', 'Task Name']]
    # Save the new DataFrame to a CSV file
    nan_rows.to_csv('nan_goal_type_rows.csv', index=False)

    project_columns = list(set(df.columns.tolist()).intersection(columns_to_check))
    # Filter rows where all columns in project_columns are equal to the string 'nan'
    nan_condition = (df[project_columns].isna() | df[project_columns].astype(str).apply(lambda column: column.str.strip().str.lower().eq('nan'))).all(axis=1)
    # Extract 'Emp Name' and 'Task Name' for rows matching the condition
    nan_rows = df[nan_condition][['Emp Name', 'Task Name']]
    # Save the extracted rows to a CSV file
    nan_rows.to_csv('nan_projects_rows.csv', index=False)

    # Remove the rows with the string 'nan' in 'Goal Type' from the original DataFrame
    df_cleaned = df[~missing_goal]
    # Remove the rows from the original DataFrame
    df_cleaned = df_cleaned[~nan_condition]

    #### write the DataFrame to an Excel file
    filename = f"allEmployeeClickUp{date_range}.xlsx"
    # Create a new Excel writer using xlsxwriter
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    df_cleaned.to_excel(writer, sheet_name='Sheet1', index=False)
    # Get the xlsxwriter workbook and worksheet objects
    worksheet = writer.sheets['Sheet1']
    # Add hyperlinks to the 'Task ID' column
    for row_num, value in enumerate(df_cleaned['Task ID'], start=1):
        if pd.isna(value):
            break
        url = f'https://app.clickup.com/t/{value}'
        worksheet.write_url(row_num, df_cleaned.columns.get_loc('Task ID'), url, string=value)
    # Save the Excel file
    writer.close()

    ### Write to CSV ###
    filename = f"allEmployeeClickUp{date_range}.csv"
    # Create a new DataFrame to store the data with hyperlinks
    df_hyperlinked = df_cleaned.copy()
    # Write the DataFrame to a CSV file
    df_hyperlinked.to_csv(filename, index=False)#, quoting=csv.QUOTE_NONE, escapechar='\\')

    shutil.rmtree(checkpoint_dir)
    print('Report complete. Saved progress cleared for the next run.')
    print(time.time()-start)


finally:
    run_lock.close()
