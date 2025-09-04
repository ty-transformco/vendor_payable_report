import re
from pathlib import Path
import datetime
import time
import pandas as pd
from openpyxl import load_workbook
import zipfile

def load_latest_ap_analysis(dir_path: str | Path) -> tuple[pd.DataFrame, Path]:
    """Load newest AP_Analysis_Report_YYYYMMDD_HHMMSS.csv from a folder.

    Args:
        dir_path (str | Path): UNC or local path to the reports folder.

    Returns:
        tuple[pd.DataFrame, Path]: The dataframe and the selected file path.
    """
    dir_path = Path(dir_path)
    ap_pattern = re.compile(r"AP_Analysis_Report_(\d{8})_(\d{6})\.csv$")
    ap_files = [f for f in dir_path.iterdir() if ap_pattern.match(f.name)]
    if not ap_files:
        raise FileNotFoundError(f"No AP_Analysis_Report_*.csv files found in {dir_path}")

    def ap_file_datetime(f: Path) -> datetime.datetime:
        m = ap_pattern.match(f.name)
        return datetime.datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S")

    latest_ap_file = max(ap_files, key=ap_file_datetime)

    # Use the fast CSV engine by passing a literal delimiter (no regex)
    df = pd.read_csv(latest_ap_file, sep="^", engine="c", dtype=str)
    return df, latest_ap_file

def load_vendor_payable_workbook(
    xlsx_path: str | Path,
    *,
    read_only: bool = False,
    data_only: bool = True,
):
    """Load the Vendor Payable Report workbook via openpyxl.

    Args:
        xlsx_path (str | Path): Absolute path to the .xlsx file.
        read_only (bool): Open in read-only mode (faster, lower memory; no saving).
        data_only (bool): If True, returns cell values instead of formulas where possible.

    Returns:
        openpyxl.workbook.workbook.Workbook: Loaded workbook object.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is not a valid .xlsx zip or appears corrupted.
        PermissionError: If the file is locked/open (e.g., by Excel or sync client).
    """
    xlsx_path = Path(xlsx_path)

    if not xlsx_path.exists():
        raise FileNotFoundError(f"Workbook not found: {xlsx_path}")

    # Quick integrity check: .xlsx should be a valid zip
    if not zipfile.is_zipfile(xlsx_path):
        raise ValueError(
            f"Not a valid .xlsx (zip) file: {xlsx_path.name}. "
            "If this is an .xlsb or corrupted file, open and re-save as .xlsx."
        )

    try:
        wb = load_workbook(filename=str(xlsx_path), read_only=read_only, data_only=data_only)
        return wb
    except zipfile.BadZipFile as e:
        raise ValueError(
            "Corrupted .xlsx (BadZipFile). In Excel, try File → Open → Open and Repair…, "
            "then Save As a new .xlsx and point the script to that file."
        ) from e
    except PermissionError as e:
        raise PermissionError(
            "Workbook appears locked (Excel open or OneDrive/SharePoint syncing). "
            "Close Excel and/or pause sync, then try again."
        ) from e

def filter_ap_analysis(
    df: pd.DataFrame,
    *,
    amount_col: str = "Amount",
    merch_col: str = "merchType",
    category_col: str = "Category",
    keep_merch_value: str = "Merch",
    keep_category_value: str = "Home Services",
) -> pd.DataFrame:
    """Apply all AP Analysis filters and return a new DataFrame.

    Args:
        df (pd.DataFrame): Raw AP Analysis DataFrame.
        amount_col (str): Column containing amounts.
        merch_col (str): Column for merch type.
        category_col (str): Column for category.
        keep_merch_value (str): Value to keep in merch_col.
        keep_category_value (str): Case-insensitive value to keep in category_col.

    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    out = df.copy()

    # --- Amount to numeric (robust): strip $, commas, and parentheses for negatives. ---
    if amount_col not in out.columns:
        raise KeyError(f"Expected column '{amount_col}' not found")

    amt = (
        out[amount_col]
        .astype(str)
        .str.strip()
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.replace("(", "-", regex=False)
        .str.replace(")", "", regex=False)
    )
    out[amount_col] = pd.to_numeric(amt, errors="coerce")
    out = out[out[amount_col].notna() & (out[amount_col] != 0)]

    # --- merchType == 'Merch' ---
    if merch_col not in out.columns:
        raise KeyError(f"Expected column '{merch_col}' not found")
    out = out[out[merch_col].fillna("").eq(keep_merch_value)]

    # --- Category == 'Home Services' (case/whitespace tolerant) ---
    if category_col not in out.columns:
        raise KeyError(f"Expected column '{category_col}' not found")
    out = out[
        out[category_col].fillna("").str.strip().str.casefold()
        == keep_category_value.casefold()
    ]

    return out

def aggregate_vendor_data_by_date(
    df: pd.DataFrame,
    start_date: str | pd.Timestamp,
    end_date: str | pd.Timestamp,
    *,
    date_col: str = "Date",
    amount_col: str = "Amount",
    account_col: str = "Account",
    type_col: str = "Type",
    vendor_col: str = "Name",
) -> pd.DataFrame:
    """_summary_
    Aggregate AP Analysis rows into vendor-level totals for a given date range.

    Business rules (exclusive; precedence Payment > Accrued Purchases > Bill > Adjustments):
      - Accrued Purchases: account in {21109, 21142} AND type in {Bill, Bill Credit, Item Receipt}
      - Bills            : account in {21142, 21110, 21117} AND type in {Vendor Bill, Bill Credit, Vendor Credit, Journal}
      - Payments         : account in {13150, 21110, 21117} AND type in {Bill Payment, Vendor Prepayment, Vendor Prepayment Application}
      - Adjustments      : Journal rows NOT matched by the Bill rule

    Args:
        df (pd.DataFrame): Filtered AP Analysis dataframe.
        start_date (str | pd.Timestamp): Start of date range (inclusive).
        end_date   (str | pd.Timestamp): End of date range (inclusive).
        date_col (str): Column containing dates to filter on.
        amount_col, account_col, type_col, vendor_col: Column names.

    Returns:
        pd.DataFrame: Columns ['Vendor', 'Accrued Purchases', 'Adjustments', 'Bill', 'Payment'].
    """
    required = {date_col, amount_col, account_col, type_col, vendor_col}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns: {sorted(missing)}")

    # --- Parse dates and filter range (inclusive) ---
    s = pd.to_datetime(start_date).normalize()
    e = pd.to_datetime(end_date).normalize()
    if pd.isna(s) or pd.isna(e):
        raise ValueError("start_date/end_date could not be parsed.")
    if s > e:
        raise ValueError("start_date cannot be after end_date.")

    tmp = df.copy()
    tmp["_date"] = pd.to_datetime(tmp[date_col], errors="coerce").dt.normalize()
    tmp = tmp[(tmp["_date"] >= s) & (tmp["_date"] <= e)]
    if tmp.empty:
        return pd.DataFrame(columns=["Vendor", "Accrued Purchases", "Adjustments", "Bill", "Payment"])

    # --- Normalize amount: $, commas, parentheses negatives ---
    amt = (
        tmp[amount_col].astype(str).str.strip()
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.replace("(", "-", regex=False)
        .str.replace(")", "", regex=False)
    )
    tmp["_amt"] = pd.to_numeric(amt, errors="coerce").fillna(0.0)

    # --- Extract leading 5-digit account code ---
    acct_code = tmp[account_col].astype(str).str.extract(r"^\s*(\d{5})", expand=False)
    tmp["_acct"] = pd.to_numeric(acct_code, errors="coerce")

    # --- Canonicalize Type ---
    t = tmp[type_col].astype(str).str.strip().str.casefold().str.replace(r"\s+", " ", regex=True)
    type_norm = t.replace({
        "bill": "vendor bill",
        "vendorbill": "vendor bill",
        "vendor  bill": "vendor bill",
        "journal entry": "journal",
        "itemreceipt": "item receipt",
        "billpayment": "bill payment",
        "vendorprepayment": "vendor prepayment",
        "vendorprepayment application": "vendor prepayment application",
    }, regex=False)
    tmp["_type"] = type_norm

    # --- Rule sets ---
    ACCRUED_ACCTS = {21109, 21142}
    BILL_ACCTS    = {21142, 21110, 21117}
    PAY_ACCTS     = {13150, 21110, 21117}

    ACCRUED_TYPES = {"vendor bill", "bill credit", "item receipt"}
    BILL_TYPES    = {"vendor bill", "bill credit", "vendor credit", "journal"}
    PAY_TYPES     = {"bill payment", "vendor prepayment", "vendor prepayment application"}

    # --- Masks ---
    accrued_mask = tmp["_acct"].isin(ACCRUED_ACCTS) & tmp["_type"].isin(ACCRUED_TYPES)
    bill_mask    = tmp["_acct"].isin(BILL_ACCTS)    & tmp["_type"].isin(BILL_TYPES)
    pay_mask     = tmp["_acct"].isin(PAY_ACCTS)     & tmp["_type"].isin(PAY_TYPES)
    journal_adj_mask = (tmp["_type"] == "journal") & ~tmp["_acct"].isin(BILL_ACCTS)

    # --- Exclusive classification with precedence ---
    cls = pd.Series("other", index=tmp.index)
    cls = cls.mask(pay_mask, "payment")
    cls = cls.mask((cls == "other") & accrued_mask, "accrued")
    cls = cls.mask((cls == "other") & bill_mask, "bill")
    cls = cls.mask((cls == "other") & journal_adj_mask, "adjustments")
    tmp["_class"] = cls

    # --- Numeric columns by class ---
    tmp["Accrued Purchases"] = tmp["_amt"].where(tmp["_class"] == "accrued", 0.0)
    tmp["Bill"]              = tmp["_amt"].where(tmp["_class"] == "bill", 0.0)
    tmp["Payment"]           = tmp["_amt"].where(tmp["_class"] == "payment", 0.0)
    tmp["Adjustments"]       = tmp["_amt"].where(tmp["_class"] == "adjustments", 0.0)

    # --- Aggregate by Vendor (Name) ---
    out = (
        tmp.groupby(vendor_col, as_index=False)[
            ["Accrued Purchases", "Adjustments", "Bill", "Payment"]
        ].sum()
    ).rename(columns={vendor_col: "Vendor"})

    return out

if __name__ == "__main__":
    t0 = time.perf_counter()

    # --- Paths ---
    AP_DIR = r"\\SHSNGSTFSX\shs_boomi_vol\Test\AP_Report_Files"
    VENDOR_DIR = (
        r"C:\Users\tbingha\Transform HoldCo LLC\Finance AI - Documents\Project docs\AP Financial Controls\Vendor Payable WeekXX - Prepare - Ali Mohdumair\Vendor Payable Report - DO NOT MODIFY\Vendor Payable Report.xlsx"
    )

    START_DATE="2025-08-18"
    END_DATE="2025-08-24"

    # --- Load ---
    df_ap_analysis_raw, ap_path = load_latest_ap_analysis(AP_DIR)
    print(f"Loaded AP Analysis file: {ap_path.name}  shape={df_ap_analysis_raw.shape}")

    # --- Filter ---
    df_ap_analysis_report = filter_ap_analysis(df_ap_analysis_raw)
    print(f"After filters: {len(df_ap_analysis_report):,} rows (from {len(df_ap_analysis_raw):,})")

    # Export if needed for testing
    # df_ap_analysis_report.to_csv("output.csv", index=False)

    # Aggregate for a specific week (edit start and end date below)
    df_aggregate_vendor_data = aggregate_vendor_data_by_date(
        df_ap_analysis_report, start_date=START_DATE, end_date=END_DATE
    )
    print(df_aggregate_vendor_data.head())

    # Open read-only if you just need to read values/sheets (faster, safer)
    wb = load_vendor_payable_workbook(VENDOR_DIR, read_only=True, data_only=True)
    print("Sheets:", wb.sheetnames)

    # Example: grab a sheet if it exists, otherwise use active
    ws_name = "APAnalysisReportByWeekResults"
    ws = wb[ws_name] if ws_name in wb.sheetnames else wb.active
    print("Using sheet:", ws.title)

    # When done (good practice even in read_only)
    wb.close()
