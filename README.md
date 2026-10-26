# Employee Statistics

Export ClickUp time entries for a chosen period, with optional task details and an Excel report. Successful API responses are saved so an interrupted run can resume.

## Setup

Python 3.10 or newer on macOS or Linux is required. Activate your usual Python environment, then install the dependencies:

```bash
python -m pip install -r requirements.txt
```

Use a ClickUp Workspace owner/admin API token to request other employees' entries. The script prompts for the token with hidden input, or reads it from `CLICKUP_API_TOKEN` if that environment variable is set. Tokens are never saved in checkpoints. Do not put tokens in the script or commit them.

## Employee selection

The script combines current workspace members with known historical user IDs from a local roster. This includes former employees whose IDs no longer appear in the current member list. The roster must be supplied privately; employee data is intentionally excluded from this repository.

Place your real `allEmpStats-roster.json` beside the script, or pass `--roster "/path/to/roster.json"`. A single legacy file named `allEmpStats-roster-*.json` beside the script is also supported. Renaming the Python script does not change roster discovery. With multiple legacy rosters, select one explicitly.

`allEmpStats-roster.example.json` documents the format using fictitious data. Replace the sample records with verified numeric ClickUp user IDs before using it. Internal employee codes alone cannot be used as API user IDs. Keep `workspace_id` consistent with the workspace being queried. The roster's `date_range` is source metadata and does not limit the reporting dates.

If you deliberately want **current members only**, use `--current-only` instead of a roster. Former employees absent from both the member API and the saved roster cannot be discovered automatically. Unresolved roster employee codes and zero-entry accounts are listed separately in the coverage CSV.

The default workspace is `3314662`; select another with `--workspace-id WORKSPACE_ID` and its matching roster.

## Run a report

For the entire September 2026 calendar month in **Indian Standard Time (IST)**:

```bash
python allEmpStats.py 2026-09-01 2026-09-30 --entries-only
```

The first date starts at 00:00:00 IST and the last date includes the complete final day through 23:59:59.999 IST. Change the two dates for another period. Dates are required; there is no silent hardcoded reporting period.

For the full Excel report, including task details and custom fields:

```bash
python allEmpStats.py 2026-09-01 2026-09-30
```

For current members only:

```bash
python allEmpStats.py 2026-09-01 2026-09-30 --current-only --entries-only
```

Unix timestamps in **seconds** are also accepted and used as absolute instants, without subtracting an IST offset:

```bash
python allEmpStats.py 1788220800 1790812799 --entries-only
```

Those example timestamps cover 1 September 2026 at 05:30 through 1 October at 05:29:59 IST. Use readable dates for a complete September in IST. A timestamp end value includes its whole final second.

See every option with `python allEmpStats.py --help`.

## Resume and refresh

Repeat the same command after an interruption to reuse completed requests. The workspace, date range and roster identify the checkpoint folder beside the script. Only one process may run against the same checkpoint at a time. Cached member and task responses are a snapshot of that run; use `--fresh` to refresh them:

```bash
python allEmpStats.py 2026-09-01 2026-09-30 --entries-only --fresh
```

`--entries-only` keeps checkpoints so the full report can reuse the time entries later. A successful full report clears its checkpoints after all exports finish. An API failure stops with the actual error and retains completed progress.

## Outputs and completeness

Files are written in the directory from which you run the command, with the chosen period in their names:

- `allEmployeeClickUp<period>_allEntries.csv`: all returned time entries, before classification filters.
- `allEmployeeClickUp<period>_employeeCoverage.csv`: per-account entry counts and unresolved employee codes.
- `allEmployeeClickUp<period>.xlsx` and `.csv`: the full report with task details, filtered using the existing goal/project classification rules.
- `nan_goal_type_rows.csv` and `nan_projects_rows.csv`: rows flagged by those classification checks.

Multiple employee codes associated with one account are displayed together; the account is queried once. Do not interpret an empty API result as proof that an employee has no historical data without reconciling it against ClickUp. The raw export and coverage file are the starting point for that reconciliation. Task/custom-field enrichment uses the API's current task metadata.

Reports, checkpoints, backups, real rosters and employee snapshots are ignored by Git. Keep them local or share through an approved private channel.

## Development checks

Tests use fictitious users and mocked API responses; they do not contact ClickUp:

```bash
python -m unittest discover -s tests -v
```
