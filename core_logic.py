import pandas as pd
import numpy as np
import re
from io import BytesIO
import openpyxl
import xlsxwriter

# ==========================================
# 0. SHARED HELPER FUNCTIONS
# ==========================================

def normalize_text(s):
    """Basic text normalization."""
    return re.sub(r"\s+|_", "", str(s).lower()) if pd.notna(s) else ""

def normalize_period_for_matching(period):
    """
    Normalize period strings so that:
      'FY 2022', 'FY22', 'FY 22'  -> 'fy22'
      'Q1 2023','Q1 23','Q1_2023' -> 'q123'
      Anything else: collapse spaces/underscores and lowercase.
    """
    if pd.isna(period):
        return None
    s = str(period).upper().strip()
    s = s.replace("_", " ")
    s = re.sub(r"\s+", " ", s)

    m = re.match(r"^(FY)\s*(\d{2}|\d{4})$", s)
    if m:
        prefix, year = m.groups()
        yy = year[-2:]
        return f"{prefix.lower()}{yy}"

    m = re.match(r"^(Q[1-4])\s*(\d{2}|\d{4})$", s)
    if m:
        prefix, year = m.groups()
        yy = year[-2:]
        return f"{prefix.lower()}{yy}"

    s = s.replace(" ", "").replace("_", "")
    return s.lower()

def find_header_row(df_preview, keywords):
    """Finds the header row index in a dataframe preview based on keywords."""
    for i, row in df_preview.iterrows():
        row_text = " ".join([str(x).lower() for x in row.values if pd.notna(x)])
        if any(k.lower() in row_text for k in keywords):
            return i
    return 0

def get_sheet_df_dynamic(file_bytes, sheet_name, keywords_for_header=["model", "key", "geography"]):
    """Reads a specific sheet, dynamically finding the header."""
    try:
        # Read a preview to find header
        preview = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name, nrows=20, header=None)
        header_row = find_header_row(preview, keywords_for_header)
        
        # Read full df
        df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name, header=header_row)
        return df
    except Exception as e:
        raise ValueError(f"Error reading sheet '{sheet_name}': {str(e)}")

def normalize_columns(df):
    """Standardizes column names based on your specific map."""
    CANONICAL_COLS = {
        "model key": "Model Key", "modelkey": "Model Key",
        "model 3.3": "Model 3.3", "model3.3": "Model 3.3",
        "geography": "Geography",
        "type": "Type",
        "level 3": "Level 3", "level3": "Level 3",
    }
    new_cols = []
    for c in df.columns:
        raw = str(c).strip()
        key = raw.lower().replace("  ", " ")
        key_nospace = key.replace(" ", "")
        if key in CANONICAL_COLS:
            new_cols.append(CANONICAL_COLS[key])
        elif key_nospace in CANONICAL_COLS:
            new_cols.append(CANONICAL_COLS[key_nospace])
        else:
            new_cols.append(raw)
    df.columns = new_cols
    return df

def get_time_cols(df):
    """Identifies time columns (FY... or Q...)."""
    cols = []
    for c in df.columns:
        s = str(c).strip()
        su = s.upper()
        if su.startswith("FY") or su.startswith("Q"):
            cols.append(c)
    return cols

# ==========================================
# 1. DMA -> FACTORS LOGIC (Code Set 1)
# ==========================================

def dma_get_options(file_bytes, sheet_name):
    """Extracts available Model Keys and Types for UI selection."""
    df = get_sheet_df_dynamic(file_bytes, sheet_name)
    df = normalize_columns(df)
    
    if "Model Key" not in df.columns or "Type" not in df.columns:
        return {"available_model_keys": [], "available_types": []}
        
    return {
        "available_model_keys": sorted(df["Model Key"].dropna().astype(str).unique()),
        "available_types": sorted(df["Type"].dropna().astype(str).unique())
    }

def dma_generate_factors(dma_file_bytes, quotes_sheet, support_sheet, 
                         total_file_bytes, total_sheet,
                         selected_model_keys, selected_types):
    """
    Implements the logic from Code Set 1 to generate the Factor file.
    """
    # 1. Load DMA Data (Quotes & Support)
    df_quotes = get_sheet_df_dynamic(dma_file_bytes, quotes_sheet)
    df_support = get_sheet_df_dynamic(dma_file_bytes, support_sheet)
    
    df_quotes = normalize_columns(df_quotes)
    df_support = normalize_columns(df_support)
    
    time_cols_q = get_time_cols(df_quotes)
    time_cols_s = get_time_cols(df_support)
    
    # 2. Process Incremental (from Quotes)
    q_incr = df_quotes[
        df_quotes["Model Key"].isin(selected_model_keys) &
        df_quotes["Type"].isin(selected_types) &
        (df_quotes["Level 3"].astype(str).str.strip().str.lower() != "actual")
    ].copy()
    
    incremental_long = q_incr.melt(
        id_vars=["Model Key", "Model 3.3", "Geography", "Type"],
        value_vars=time_cols_q,
        var_name="Time Period", value_name="Incremental Volume"
    )
    
    # 3. Process Support
    s_filtered = df_support[
        df_support["Model Key"].isin(selected_model_keys) &
        df_support["Type"].isin(selected_types)
    ].copy()
    
    support_long = s_filtered.melt(
        id_vars=["Model Key", "Model 3.3", "Geography", "Type"],
        value_vars=time_cols_s,
        var_name="Time Period", value_name="Support"
    )
    # Support Share
    support_long["Total Support Var Period"] = support_long.groupby(["Model Key", "Model 3.3", "Type", "Time Period"])["Support"].transform("sum")
    support_long["Support Share"] = support_long["Support"] / support_long["Total Support Var Period"]
    
    # 4. Process Actuals (Sales)
    actual_mask = (df_quotes["Model Key"].isin(selected_model_keys)) & \
                  (df_quotes["Level 3"].astype(str).str.strip().str.lower() == "actual")
    actual_df = df_quotes[actual_mask].copy()
    
    sales_long = actual_df.melt(
        id_vars=["Model Key", "Geography"],
        value_vars=time_cols_q,
        var_name="Time Period", value_name="Actual"
    )
    sales_long["Total_Actual_Period"] = sales_long.groupby(["Model Key", "Time Period"])["Actual"].transform("sum")
    sales_long["Sales_Share"] = sales_long["Actual"] / sales_long["Total_Actual_Period"]
    
    # 5. Process Predicted (if exists)
    pred_mask = (df_quotes["Model Key"].isin(selected_model_keys)) & \
                (df_quotes["Level 3"].astype(str).str.strip().str.lower() == "predicted")
    pred_df = df_quotes[pred_mask].copy()
    
    if not pred_df.empty:
        pred_long = pred_df.melt(
            id_vars=["Model Key", "Model 3.3", "Geography"],
            value_vars=time_cols_q,
            var_name="Time Period", value_name="Predicted_Volume"
        )
    else:
        # fallback if no predicted rows
        pred_long = pd.DataFrame(columns=["Model Key", "Model 3.3", "Geography", "Time Period", "Predicted_Volume"])

    # 6. Merge Main Data
    merged = incremental_long.merge(
        support_long[["Model Key", "Model 3.3", "Geography", "Type", "Time Period", "Support", "Support Share"]],
        on=["Model Key", "Model 3.3", "Geography", "Type", "Time Period"], how="left"
    )
    merged = merged.merge(
        sales_long[["Model Key", "Geography", "Time Period", "Actual", "Sales_Share"]],
        on=["Model Key", "Geography", "Time Period"], how="left"
    )
    if not pred_long.empty:
        merged = merged.merge(
            pred_long[["Model Key", "Geography", "Time Period", "Predicted_Volume"]],
            on=["Model Key", "Geography", "Time Period"], how="left"
        )
    
    # 7. Expected Volume Share
    merged["Support_Sales_Score"] = merged["Support Share"] * merged["Sales_Share"]
    merged["Score_Total_Period"] = merged.groupby(["Model Key", "Model 3.3", "Type", "Time Period"])["Support_Sales_Score"].transform("sum")
    merged["Expected_Volume_Share"] = merged["Support_Sales_Score"] / merged["Score_Total_Period"]
    
    merged["Support_Sales_Ratio"] = np.where(
        merged["Support Share"] < 1,
        np.where(merged["Sales_Share"].abs() < 1e-9, 1, merged["Support Share"] / merged["Sales_Share"]),
        1
    )
    merged["CN11"] = 1 + merged["Sales_Share"]
    
    # 8. Load TOTAL INCREMENTAL (External File)
    # We treat the total file similar to dma file
    df_total = get_sheet_df_dynamic(total_file_bytes, total_sheet)
    df_total = normalize_columns(df_total)
    time_cols_total = get_time_cols(df_total)
    
    # Filter
    t_filtered = df_total[
        df_total["Model Key"].isin(selected_model_keys) &
        df_total["Type"].isin(selected_types)
    ].copy()
    
    total_long = t_filtered.melt(
        id_vars=["Model Key", "Model 3.3", "Type"],
        value_vars=time_cols_total,
        var_name="Time Period", value_name="Total_Incremental_Period"
    )
    
    # Group sum (across DMAs in the total file if necessary)
    total_inc_grouped = total_long.groupby(["Model Key", "Model 3.3", "Type", "Time Period"], as_index=False)["Total_Incremental_Period"].sum()
    
    # Merge Total Incremental
    merged = merged.merge(total_inc_grouped, on=["Model Key", "Model 3.3", "Type", "Time Period"], how="left")
    
    # 9. Final Calculations
    merged["Expected_Volume"] = merged["Expected_Volume_Share"] * merged["Total_Incremental_Period"]
    
    # ** NEW: Total Expected across all geographies **
    if "Model Key" in merged.columns and "Expected_Volume" in merged.columns:
        merged['Total Expected'] = merged.groupby('Model Key')['Expected_Volume'].transform('sum')

    merged["Percentage_Contribution"] = np.where(
        merged.get("Predicted_Volume", 0).abs() < 1e-9, np.nan,
        merged["Incremental Volume"] / merged["Predicted_Volume"]
    )
    
    merged["Ratio_Shr_Support_vs_Shr_Sales"] = merged["Support_Sales_Ratio"] * merged["CN11"]
    
    # Total Predicted
    group_cols_pred = [c for c in ["Model Key", "Model 3.3", "Type", "Time Period"] if c in merged.columns]
    merged["Total_Predicted_Volume"] = merged.groupby(group_cols_pred)["Predicted_Volume"].transform("sum")
    
    merged["Total_Percentage_Contribution_Var_Period"] = merged["Total_Incremental_Period"] / merged["Total_Predicted_Volume"]
    
    merged["Adjusted_Contribution"] = np.where(
        merged["Total_Percentage_Contribution_Var_Period"].abs() < 1e-9, np.nan,
        merged["Ratio_Shr_Support_vs_Shr_Sales"] * merged["Total_Percentage_Contribution_Var_Period"]
    )
    
    merged["adj_contri_2"] = merged["Adjusted_Contribution"] * merged["Predicted_Volume"]
    
    merged["Total_adj_contri_2_Period_Var"] = merged.groupby(["Model Key", "Model 3.3", "Type", "Time Period"])["adj_contri_2"].transform("sum")
    merged["Normalized_adj_contri_2"] = merged["adj_contri_2"] / merged["Total_adj_contri_2_Period_Var"]
    
    merged["Total_Expected_Volume"] = merged.groupby(["Model Key", "Model 3.3", "Type", "Time Period"])["Expected_Volume"].transform("sum")
    merged["Rescaled_Adj_Contribution"] = merged["Normalized_adj_contri_2"] * merged["Total_Expected_Volume"]
    
    merged["Factor"] = np.where(
        (merged["Rescaled_Adj_Contribution"].abs() < 1e-9) | (merged["Incremental Volume"].abs() < 1e-9),
        1,
        merged["Rescaled_Adj_Contribution"] / merged["Incremental Volume"]
    )
    
    # 10. Output
    factors_output = merged[[
        "Model Key", "Model 3.3", "Geography", "Type", "Time Period",
        "Incremental Volume", "Expected_Volume", "Total Expected", "Percentage_Contribution",
        "Support_Sales_Ratio", "Ratio_Shr_Support_vs_Shr_Sales",
        "Total_Percentage_Contribution_Var_Period", "Adjusted_Contribution",
        "Factor", "Normalized_adj_contri_2"
    ]].copy()
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        merged.to_excel(writer, sheet_name='Detailed', index=False)
        factors_output.to_excel(writer, sheet_name='Factors', index=False)
        
    return output.getvalue(), f"Factors Generated. Rows: {len(merged)}"

# ==========================================
# 2. FACTOR PROCESSING LOGIC (Code Set 2)
# ==========================================

def process_factor_file(factor_bytes, periods_to_remove=None):
    """
    Reads the Factor file, allows filtering periods, and builds lookup maps.
    """
    df_factors = pd.read_excel(BytesIO(factor_bytes), sheet_name="Factors", engine="openpyxl")
    
    # Clean cols
    for c in ["Model 3.3", "Geography", "Time Period"]:
        if c in df_factors.columns:
            df_factors[c] = df_factors[c].astype(str).str.strip()
            
    rows_before = len(df_factors)
    
    # Filter periods if requested
    if periods_to_remove:
        df_factors = df_factors[~df_factors["Time Period"].isin(periods_to_remove)].copy()
        
    rows_after = len(df_factors)
    
    # Build Map
    factor_map = {}
    factor_rows = [] 
    
    for _, r in df_factors.iterrows():
        var = str(r.get("Model 3.3", "")).strip()
        geo = str(r.get("Geography", "")).strip()
        period_raw = r.get("Time Period", "")
        period_key = normalize_period_for_matching(period_raw)
        
        try:
            fval = float(r.get("Factor", 1))
        except:
            continue
            
        if not var or not geo or not period_key:
            continue
            
        key = (var, geo, period_key)
        factor_map[key] = fval
        factor_rows.append((var, geo, period_raw, period_key, fval))
        
    # Output file (updated)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_factors.to_excel(writer, sheet_name='Factors', index=False)
        
    summary = {
        "periods_available": sorted(df_factors["Time Period"].unique().tolist()),
        "rows_before": rows_before,
        "rows_after": rows_after,
        "map_size": len(factor_map)
    }
    
    return output.getvalue(), factor_rows, factor_map, summary

# ==========================================
# 3. GRANULAR UPDATE LOGIC (Code Set 3)
# ==========================================

def update_granular_with_factors(factor_rows, factor_map, granular_bytes, lower, upper):
    """
    Updates the Granular OVERRIDE sheet based on factors.
    """
    wb = pd.ExcelFile(BytesIO(granular_bytes))
    
    # Find OVERRIDE sheet
    override_sheet_name = next((s for s in wb.sheet_names if s.lower() == "override"), None)
    if not override_sheet_name:
        raise ValueError("No 'OVERRIDE' sheet found in Granular file.")
        
    df_override = pd.read_excel(wb, sheet_name=override_sheet_name)
    
    multiplied_log = []
    skipped_log = []
    
    for idx, row in df_override.iterrows():
        var = str(row.get("Variable", "")).strip()
        geo = str(row.get("Geography", "")).strip()
        contrib = row.get("Contribution", "")
        
        if not var or not geo or pd.isna(contrib):
            skipped_log.append([var, geo, contrib, "Missing keys"])
            continue
            
        period_key = normalize_period_for_matching(contrib)
        key = (var, geo, period_key)
        fval = factor_map.get(key)
        
        if fval is None:
            skipped_log.append([var, geo, contrib, "No match found"])
            continue
            
        # Tolerance check
        if (lower <= fval <= upper) or fval == 1:
            skipped_log.append([var, geo, contrib, f"Within tolerance ({fval:.4f})"])
            continue
            
        # Update
        try:
            old_min = float(row.get("Min"))
            old_max = float(row.get("Max"))
        except:
            skipped_log.append([var, geo, contrib, "Min/Max not numeric"])
            continue
            
        df_override.at[idx, "Min"] = old_min * fval
        df_override.at[idx, "Max"] = old_max * fval
        multiplied_log.append([var, geo, contrib, old_min, old_max, df_override.at[idx, "Min"], df_override.at[idx, "Max"], fval])
        
    # Create Outputs (using openpyxl to preserve other sheets if possible, 
    # but pandas ExcelWriter recreation is safer for stateless usage)
    output_granular = BytesIO()
    with pd.ExcelWriter(output_granular, engine='openpyxl') as writer:
        for sheet in wb.sheet_names:
            if sheet == override_sheet_name:
                df_override.to_excel(writer, sheet_name=sheet, index=False)
            else:
                pd.read_excel(wb, sheet_name=sheet).to_excel(writer, sheet_name=sheet, index=False)
                
    # Logs
    log_multiplied = pd.DataFrame(multiplied_log, columns=["Variable", "Geography", "Contribution", "Old_Min", "Old_Max", "New_Min", "New_Max", "Factor"])
    log_skipped = pd.DataFrame(skipped_log, columns=["Variable", "Geography", "Contribution", "Reason"])
    
    output_log = BytesIO()
    with pd.ExcelWriter(output_log, engine='xlsxwriter') as writer:
        log_multiplied.to_excel(writer, sheet_name="Multiplied", index=False)
        log_skipped.to_excel(writer, sheet_name="Skipped", index=False)
        
    summary = {"multiplied_count": len(log_multiplied), "skipped_count": len(log_skipped)}
    
    return output_granular.getvalue(), output_log.getvalue(), summary, df_override

# ==========================================
# 4. ADS UPDATE LOGIC (Code Set 4)
# ==========================================

def update_ads_with_factors(master_spec_bytes, ads_bytes, ads_is_csv, factor_rows, granular_df, lower, upper):
    """
    Updates the ADS file using factors and master spec mapping.
    (Includes robust dynamic header finding for Master Spec)
    """
    # --- Helper specific to this function ---
    def detect_col_by_substring(cols, substrings):
        for c in cols:
            low = str(c).lower()
            for s in substrings:
                if s.lower() in low:
                    return c
        return None

    # 1. Load Master Spec
    # Use find_header_row helper to ensure we skip empty rows/notes at the top
    preview = pd.read_excel(BytesIO(master_spec_bytes), sheet_name="Model Specifications", nrows=40, header=None, engine='openpyxl')
    
    # We look for a row that contains BOTH "variable" and "include" to be safe
    hdr_master = find_header_row(preview, ["variable", "include"])
    
    # Reload with the correct header row
    master_df = pd.read_excel(
        BytesIO(master_spec_bytes), 
        sheet_name="Model Specifications", 
        header=hdr_master,
        dtype=str, 
        engine='openpyxl'
    )
    master_df.columns = [str(c).strip() for c in master_df.columns]

    # --- Robust Column Mapping ---
    col_map = {str(c).strip().lower(): c for c in master_df.columns}
    
    # Priority lists for column names based on your error log context
    preferred_var_names = ["variables", "variable", "variables_list", "variable name"]
    preferred_include_names = ["include variables", "include variable", "include", "include_variable", "include?"]
    preferred_pmf_names = ["post multiplication", "post_multiplication", "postmultiplication", "pmf", "post-multiplication"]

    def pick_preferred(colmap, candidates):
        for cand in candidates:
            if cand.lower() in colmap:
                return colmap[cand.lower()]
        return None

    # Attempt to find exact/preferred matches first
    var_col = pick_preferred(col_map, preferred_var_names)
    inc_col = pick_preferred(col_map, preferred_include_names)
    pmf_col_header = pick_preferred(col_map, preferred_pmf_names)

    # Fallback to substring search if preferred names aren't found
    if var_col is None:
        var_col = detect_col_by_substring(master_df.columns, ["variable"])
    if inc_col is None:
        inc_col = detect_col_by_substring(master_df.columns, ["include"])
    if pmf_col_header is None:
        pmf_col_header = detect_col_by_substring(master_df.columns, ["post", "pmf"])

    # Final check
    if not all([var_col, inc_col, pmf_col_header]):
        available = list(master_df.columns)
        raise ValueError(
            f"Could not find required columns in Master Spec.\n"
            f"Looking for keywords: Variable, Include, Post Multiplication.\n"
            f"Found headers at row {hdr_master}: {available}\n"
            f"Please ensure the Master Spec has these columns."
        )
        
    var_to_pmf = {}
    for _, r in master_df.iterrows():
        # Check "Include" column (handling various 'yes' formats)
        if str(r.get(inc_col, "")).lower() in ["y", "yes", "true", "1"]:
            v = str(r.get(var_col, "")).strip()
            p = str(r.get(pmf_col_header, "")).strip()
            if v and p:
                var_to_pmf[v] = p
                
    # 2. Load ADS
    if ads_is_csv:
        preview = pd.read_csv(BytesIO(ads_bytes), nrows=40, header=None)
        h = find_header_row(preview, ["quarter", "time", "geo"])
        df_ads = pd.read_csv(BytesIO(ads_bytes), header=h, dtype=object)
    else:
        preview = pd.read_excel(BytesIO(ads_bytes), nrows=40, header=None)
        h = find_header_row(preview, ["quarter", "time", "geo"])
        df_ads = pd.read_excel(BytesIO(ads_bytes), header=h, dtype=object)

    df_ads.columns = [str(c).strip() for c in df_ads.columns]

    # Find ADS cols using substring detection
    ads_geo_col = detect_col_by_substring(df_ads.columns, ["geo", "geography"])
    ads_time_col = detect_col_by_substring(df_ads.columns, ["quarter", "time"])
    
    if not ads_geo_col or not ads_time_col:
        raise ValueError("Could not find Geography or Time Period columns in ADS.")
        
    # Pre-compute normalized time for ADS
    ads_time_norm = df_ads[ads_time_col].apply(normalize_period_for_matching)
    
    # 3. Build Contribution Map (to skip granular contrib period)
    contrib_map = {}
    if granular_df is not None:
        for _, r in granular_df.iterrows():
            v = str(r.get("Variable", "")).strip()
            g = str(r.get("Geography", "")).strip()
            c = r.get("Contribution", "")
            ck = normalize_period_for_matching(c)
            if v and g and ck:
                contrib_map[(v, g)] = ck
                
    # 4. Apply Factors
    multiplied_log = []
    skipped_log = []
    
    for (var, geo, period_raw, period_key, fval) in factor_rows:
        # Check Master Spec Inclusion
        if var not in var_to_pmf:
            skipped_log.append([var, geo, period_raw, "Not in Master Spec"])
            continue
            
        target_col = var_to_pmf[var]
        if target_col not in df_ads.columns:
            skipped_log.append([var, geo, period_raw, f"Column '{target_col}' missing in ADS"])
            continue
            
        # Check Contrib Skip
        if contrib_map.get((var, geo)) == period_key:
            skipped_log.append([var, geo, period_raw, "Skipped (Contribution Period)"])
            continue
            
        # Tolerance
        if (lower <= fval <= upper) or fval == 1:
            skipped_log.append([var, geo, period_raw, f"Within tolerance ({fval:.4f})"])
            continue
            
        # Apply
        mask = (ads_time_norm == period_key) & (df_ads[ads_geo_col].astype(str).str.strip() == geo)
        
        if mask.any():
            # Convert to numeric, force errors to NaN, then fill NaNs with 1
            df_ads[target_col] = pd.to_numeric(df_ads[target_col], errors='coerce').fillna(1)
            
            # Apply multiplication
            df_ads.loc[mask, target_col] = df_ads.loc[mask, target_col] * fval
            multiplied_log.append([var, geo, period_raw, target_col, fval, mask.sum()])
        else:
            skipped_log.append([var, geo, period_raw, "No matching ADS rows"])

    # 5. Outputs
    output_ads = BytesIO()
    if ads_is_csv:
        df_ads.to_csv(output_ads, index=False, encoding="utf-8-sig")
    else:
        df_ads.to_excel(output_ads, index=False)
        
    log_mult_df = pd.DataFrame(multiplied_log, columns=["Variable", "Geography", "Period", "Column", "Factor", "Rows"])
    log_skip_df = pd.DataFrame(skipped_log, columns=["Variable", "Geography", "Period", "Reason"])
    
    output_log = BytesIO()
    with pd.ExcelWriter(output_log, engine='xlsxwriter') as writer:
        log_mult_df.to_excel(writer, sheet_name="Multiplied", index=False)
        log_skip_df.to_excel(writer, sheet_name="Skipped", index=False)
        
    summary = {
        "total_factor_rows": len(factor_rows),
        "total_applied": len(log_mult_df),
        "total_skipped": len(log_skip_df)
    }
    
    return output_ads.getvalue(), output_log.getvalue(), summary
