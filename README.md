# README — AP Analysis → Vendor Payable Inserter (CLI Dates, Run Modes, Monday Gate)

This script loads the latest **AP_Analysis_Report_YYYYMMDD_HHMMSS.csv** from a folder, filters/aggregates it, and posts weekly and/or daily flows into an Excel **Vendor Payable Report** workbook.

It supports:
- **Auto** or **manual** date selection
- **Run modes** to execute **only weekly**, **only daily**, or **both** (default)
- Optional **timezone** for “today” when computing auto dates
- **Monday gate**: weekly steps run **only on Mondays** (in `--tz`) **unless** you pass `--force-weekly`

---

## Quick Start

Most people can just run:
```bash
python your_script.py
```
- Uses **auto dates** (see below) and runs **both** weekly and daily.
- On **non‑Mondays**, the **weekly** portion is **skipped by default** (Monday gate). Add `--force-weekly` to run it anyway.

---

## How dates are chosen

### Auto dates (default)
- `DAILY_DATE` = **today** (in `--tz`, default `America/New_York`)
- `START_DATE` / `END_DATE` = **previous** week’s **Sunday → Saturday** window

Example: If today is **Monday 9/15/2025**, then
- `START_DATE = 9/7/2025` (previous Sunday)
- `END_DATE   = 9/13/2025` (previous Saturday)
- `DAILY_DATE = 9/15/2025` (today)

Run with explicit auto flag or just omit manual args:
```bash
python your_script.py --auto-dates
# or simply
python your_script.py
```

Specify a timezone (IANA name) if needed:
```bash
python your_script.py --auto-dates --tz America/Chicago
```

### Manual dates
Provide all three:
```bash
python your_script.py --manual-dates --start 2025-09-07 --end 2025-09-13 --daily 2025-09-15
# Manual mode is also implied if you supply any of --start/--end/--daily:
python your_script.py --start 9/7/2025 --end 9/13/2025 --daily 9/15/2025
```
Accepted formats: `YYYY-MM-DD`, `M/D/YYYY`. Validation ensures `start ≤ end`.

---

## Run modes (choose which parts to run)

- Run **both** (default): do weekly *and* daily
  ```bash
  python your_script.py
  ```

- **Weekly only**:
  ```bash
  python your_script.py --only-weekly
  # with auto dates explicitly:
  python your_script.py --auto-dates --only-weekly
  ```

- **Daily only**:
  ```bash
  python your_script.py --only-daily
  # with manual dates:
  python your_script.py --start 9/15/2025 --end 9/15/2025 --daily 9/15/2025 --only-daily
  ```

---

## Monday gate (weekly only on Mondays, unless overridden)

- By default, the **weekly** steps run **only if today is Monday** in the selected timezone (`--tz`, default `America/New_York`).  
- On other days, weekly steps are **skipped** automatically.
- To **override** and run weekly **any day**, add `--force-weekly`.

Examples:
```bash
# Non‑Monday, run both: weekly is skipped automatically; daily still runs
python your_script.py

# Non‑Monday, force weekly to run anyway
python your_script.py --force-weekly

# Monday, but you want daily only
python your_script.py --only-daily

# Weekly only on a non‑Monday (forced)
python your_script.py --only-weekly --force-weekly
```

---

## Prerequisites

- **Python 3.11+** (3.12 recommended)
- Packages:
  ```bash
  pip install pandas openpyxl
  ```
- Access to your network path containing AP reports and the Excel workbook.
- **Excel must be closed** (the script writes to the workbook).

---

## Configure paths (once)

Open the script and set these two paths near the top of `__main__`:

```python
AP_DIR = r"\\SHSNGSTFSX\shs_boomi_vol\Test\AP_Report_Files"
VENDOR_DIR = r"C:\Users\tbingha\Transform HoldCo LLC\Finance AI - Documents\Project docs\AP Financial Controls\Vendor Payable WeekXX - Prepare - Ali Mohdumair\Vendor Payable Report - DO NOT MODIFY\Vendor Payable Report.xlsx"
```

> Keep these as raw strings (`r"..."`) so backslashes work on Windows.

---

## Typical ways to run

**Auto dates, both weekly & daily (default):**
```bash
python your_script.py
```

**Auto dates, weekly only (Monday required unless forced):**
```bash
python your_script.py --only-weekly
# force on non‑Monday:
python your_script.py --only-weekly --force-weekly
```

**Auto dates, daily only (today):**
```bash
python your_script.py --only-daily
```

**Manual dates, both:**
```bash
python your_script.py --manual-dates --start 2025-09-07 --end 2025-09-13 --daily 2025-09-15
```

**Manual dates, weekly only (force if not Monday):**
```bash
python your_script.py --manual-dates --start 2025-09-07 --end 2025-09-13 --daily 2025-09-15 --only-weekly --force-weekly
```

**Auto with a different timezone:**
```bash
python your_script.py --auto-dates --tz America/Chicago
```

---

## VS Code: run with arguments

Create **`.vscode/launch.json`** with one or more entries, e.g.:

**Auto dates, weekly only (force on non‑Monday):**
```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "AP Inserter (auto, weekly only, forced)",
      "type": "python",
      "request": "launch",
      "program": "${workspaceFolder}/your_script.py",
      "console": "integratedTerminal",
      "args": ["--auto-dates", "--only-weekly", "--force-weekly"]
    }
  ]
}
```

**Manual dates, daily only:**
```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "AP Inserter (manual, daily only)",
      "type": "python",
      "request": "launch",
      "program": "${workspaceFolder}/your_script.py",
      "console": "integratedTerminal",
      "args": [
        "--manual-dates",
        "--start", "2025-09-07",
        "--end",   "2025-09-13",
        "--daily", "2025-09-15",
        "--only-daily"
      ]
    }
  ]
}
```

---

## What the script does

1. **Loads** the newest `AP_Analysis_Report_*.csv` from `AP_DIR`.
2. **Filters** rows (Merch only, cleans amounts, consolidates categories → `Home Services` / `Brands & Retail`).
3. **Weekly (if enabled & Monday or forced)** for `START_DATE..END_DATE` and **inserts** into **Listings**:
   - Adds **Accrued Purchases / Adjustments / Bill / Payment** columns,
   - Creates a new **`Period X- Week Y`** column,
   - Updates **Net Change** and extends **Totals** rows.
4. **Daily (if enabled)** for `DAILY_DATE` and **inserts** into **Daily Listings**:
   - Adds the same four flow columns and a new **`MM/DD/YYYY`** date column,
   - Updates **Net Change** and extends **Totals** rows.
5. **Writes summaries**:
   - **Weekly Summary** (when weekly runs; banner shows `Week MM/DD/YYYY to MM/DD/YYYY`),
   - **Daily Summary** (when daily runs; banner shows `MM/DD/YYYY`).
6. **Saves** the workbook and (if daily runs) emits a **daily CSV** in the working directory:
   ```
   df_aggregate_vendor_data_daily_YYYY-MM-DD.csv
   ```

---

## Command-line options (recap)

### Date selection
- `--auto-dates` (default behavior if no manual args given)
- `--manual-dates` (or supplying any of `--start/--end/--daily` requires all three)
- `--start <date>`  Manual start date (inclusive)
- `--end <date>`    Manual end date (inclusive)
- `--daily <date>`  Manual daily posting date
- `--tz <IANA>`     Timezone for “today” in auto mode (default: `America/New_York`)

### Run mode
- *(default)* run both weekly and daily
- `--only-weekly`   Run weekly operations only
- `--only-daily`    Run daily operations only

### Monday gate
- *(default)* Weekly only runs on Mondays in `--tz`
- `--force-weekly`  Override Monday-only rule and run weekly on any day

Accepted date formats: `YYYY-MM-DD`, `M/D/YYYY`.

---

## Troubleshooting

- **`No AP_Analysis_Report_*.csv files found`**  
  Check `AP_DIR` and the file naming pattern. Confirm the share is reachable.

- **`Workbook appears locked` / `PermissionError`**  
  Close Excel and pause OneDrive/SharePoint sync if needed. The script needs exclusive write access.

- **`Could not find 'Net Change' header` / `Could not locate 'Period X- Week Y'` / `Could not find any 'MM/DD/YYYY'`**  
  Verify sheet names (**Listings**, **Daily Listings**) and that your template headers are intact:
  - Row 1 must start with `Division`, `Vendor`, and contain **Accrued Purchases / Adjustments / Bill / Payment / Net Change**.
  - For weekly, at least one existing `Period X- Week Y` column must be present before **Net Change**.
  - For daily, at least one existing date column `MM/DD/YYYY` must be present before **Net Change**.

- **`Missing required columns: ...`**  
  The AP report must include `Date`, `Amount`, `Account`, `Type`, `Name`, `Category`, and `merchType`. Confirm the export matches expectations.

- **Numbers red but positive / formatting oddities**  
  The script formats negatives red and uses `#,##0.00`. If your template applies conflicting styles, clear them for the new columns.

- **Duplicate postings**  
  Running the same window/day twice inserts new columns again and re-applies flows. If you need idempotence, either revert the workbook or add guards in your process.

---

## Notes & assumptions

- The script infers the latest **AP Analysis** file by parsing the timestamp in the filename and picks the max.
- Amount strings are normalized (`$`, `,`, parentheses for negatives).
- Transaction classification precedence: **Payment > Accrued Purchases > Bill > Adjustments**.
- The **Weekly Summary** banner shows “Week A to B” when start ≠ end; Daily shows just the date.

---

## Change log

- **Monday gate**: Weekly runs only on Mondays by default; override with `--force-weekly`.
- **Run modes**: `--only-weekly`, `--only-daily` to limit execution scope.
- **Auto dates**: `DAILY_DATE=today`; `START/END=previous Sunday–Saturday` (timezone-aware via `--tz`).
- **Manual dates**: `--manual-dates` with `--start/--end/--daily` (formats: `YYYY-MM-DD`, `M/D/YYYY`).

---

## Contact / Next tweaks

- Make `AP_DIR` / `VENDOR_DIR` CLI-configurable? Easy add.  
- Add a dry-run flag to skip Excel writes and only emit CSV? Straightforward.  
- Persist typical date windows in multiple VS Code launch configs for one-click runs.

Happy posting!
