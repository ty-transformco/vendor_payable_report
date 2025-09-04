# ************************Convert to openpyxl*******************************
def add_next_period_week_column(df_listings: pd.DataFrame,
                                is_53_week: bool = False,
                                fill_value=pd.NA):
    """
    Adds a new column named 'Period X- Week Y' to df_listings.
    - X (Period) is determined by which 4-5-4 fiscal period the week Y falls into.
    - Y (Week) is the max existing Week + 1 among columns matching 'Period X- Week Y'.

    Args:
        df_listings: DataFrame with columns like 'Period 5- Week 20', etc.
        is_53_week: Set True if this fiscal year has 53 weeks.
        fill_value: Value used to initialize the new column.

    Returns:
        (df_listings, new_col_name)
    """
    # 1) Extract week numbers from existing 'Period X- Week Y' columns
    pattern = re.compile(r"Period\s*\d+\s*-\s*Week\s*(\d+)", flags=re.IGNORECASE)
    weeks = []
    for col in map(str, df_listings.columns):
        m = pattern.search(col)
        if m:
            weeks.append(int(m.group(1)))

    if not weeks:
        raise ValueError("No columns matching 'Period X- Week Y' were found.")

    last_week = max(weeks)
    next_week = last_week + 1

    # 2) Determine allowable max week and 4-5-4 period boundaries
    max_week_allowed = 53 if is_53_week else 52
    if next_week > max_week_allowed:
        raise ValueError(f"Next week ({next_week}) exceeds the {max_week_allowed}-week fiscal year.")

    period_ends = [4, 9, 13, 17, 22, 26, 30, 35, 39, 43, 48, max_week_allowed]  # 4-5-4 pattern

    # 3) Map week -> period number
    period = next(i + 1 for i, end in enumerate(period_ends) if next_week <= end)

    # 4) Add the new column
    new_col = f"Period {period}- Week {next_week}"
    df_listings[new_col] = fill_value
    return df_listings, new_col

# --- Usage ---
df_listings, new_col = add_next_period_week_column(df_listings)
print("Added column:", new_col)