import streamlit as st
import pandas as pd
from io import BytesIO

from core_logic import (
    dma_get_options,
    dma_generate_factors,
    process_factor_file,
    update_granular_with_factors,
    update_ads_with_factors,
)

# =========================
# Page config + AMFAM animation
# =========================

st.set_page_config(
    page_title="AMFAM Factor & ADS Wizard",
    layout="wide",
)

# --- AMFAM animated header ---
st.markdown(
    """
    <style>
    .amfam-container {
        position: relative; height: 60px; margin: 10px 0 20px 0;
        display: flex; justify-content: center; align-items: center;
    }
    .amfam-row { display: flex; gap: 0.75rem; }
    @keyframes fadeCycle {
        0%   { opacity: 0; transform: translateY(10px); }
        40%  { opacity: 1; transform: translateY(0); }
        80%  { opacity: 0; transform: translateY(-5px); }
        100% { opacity: 0; transform: translateY(10px); }
    }
    .amfam-char {
        opacity: 0; display: inline-block; animation: fadeCycle 2.5s infinite;
        font-weight: 800; font-size: 3rem; color: #3b82f6; font-family: monospace;
    }
    .char-0 { animation-delay: 0.0s; }
    .char-1 { animation-delay: 0.1s; }
    .char-2 { animation-delay: 0.2s; }
    .char-3 { animation-delay: 0.3s; }
    .char-4 { animation-delay: 0.4s; }
    
    .amfam-overlay {
        position: fixed; inset: 0; background: rgba(15, 23, 42, 0.9);
        backdrop-filter: blur(4px); z-index: 9999;
        display: flex; flex-direction: column; align-items: center; justify-content: center;
    }
    .amfam-pulse-text {
        color: #e5e7eb; font-size: 1.1rem; font-weight: 500;
        text-align: center; animation: pulse 1.5s ease-in-out infinite; margin-top: 0.75rem;
    }
    @keyframes pulse { 0%, 100% { opacity: 0.6; } 50% { opacity: 1.0; } }
    </style>
    
    <div class="amfam-container">
        <div class="amfam-row">
            <span class="amfam-char char-0">A</span>
            <span class="amfam-char char-1">M</span>
            <span class="amfam-char char-2">F</span>
            <span class="amfam-char char-3">A</span>
            <span class="amfam-char char-4">M</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

def amfam_loader(text: str = "Processing..."):
    return f"""
    <div class="amfam-overlay">
        <div class="amfam-row" style="margin-bottom: 1.5rem;">
            <span class="amfam-char char-0">A</span><span class="amfam-char char-1">M</span>
            <span class="amfam-char char-2">F</span><span class="amfam-char char-3">A</span>
            <span class="amfam-char char-4">M</span>
        </div>
        <p class="amfam-pulse-text">{text}</p>
    </div>
    """

st.title("Factor & ADS Wizard – AMFAM")

st.write(
    "Use the tabs below in order: **DMA → Factor file** (optional) → "
    "**Factor Periods** → **Granular OVERRIDE** → **ADS Update**."
)

# ------------------------
# Session state init
# ------------------------
state_keys = [
    "factor_summary", "factor_bytes", "factor_name", 
    "granular_bytes", "updated_granular_bytes", 
    "granular_log_bytes", "granular_summary", "granular_preview",
    "ads_updated_bytes", "ads_log_bytes", "ads_summary",
    "factor_processed_bytes", "factor_processed_summary",
    "dma_model_keys", "dma_types"
]

for key in state_keys:
    if key not in st.session_state:
        st.session_state[key] = None

if "dma_model_keys" not in st.session_state or st.session_state.dma_model_keys is None:
    st.session_state.dma_model_keys = []
if "dma_types" not in st.session_state or st.session_state.dma_types is None:
    st.session_state.dma_types = []

# --- NEW: CACHE RESET FUNCTION ---
def reset_all_state():
    """Wipes all processed data when a new file is uploaded."""
    for key in state_keys:
        st.session_state[key] = None
    st.session_state.dma_model_keys = []
    st.session_state.dma_types = []

# =========================
# Tabs UI
# =========================

tab_dma, tab_factor, tab_granular, tab_ads = st.tabs(
    ["DMA → Factor file", "Factor Periods", "Granular OVERRIDE", "ADS Update"]
)

# ======================================================
# TAB 1: DMA → FACTORS
# ======================================================
with tab_dma:
    st.header("Step 0 (Optional): DMA Summary + Total Incremental → Factors")
    
    col_dma1, col_dma2 = st.columns(2)
    
    with col_dma1:
        # Added on_change=reset_all_state
        dma_file = st.file_uploader(
            "1. Upload DMA Summary (Actuals/Support)", 
            type=["xlsx", "xls", "xlsb"], 
            key="dma_main",
            on_change=reset_all_state 
        )
    with col_dma2:
        # Added on_change=reset_all_state
        total_file = st.file_uploader(
            "2. Upload Total Incremental File", 
            type=["xlsx", "xls", "xlsb"], 
            key="dma_total",
            on_change=reset_all_state
        )

    if dma_file and total_file:
        try:
            # Load sheets for dropdowns
            xls_dma = pd.ExcelFile(dma_file)
            dma_sheets = xls_dma.sheet_names
            
            xls_total = pd.ExcelFile(total_file)
            total_sheets = xls_total.sheet_names
            
            # Smart defaults
            quotes_default = next((i for i, s in enumerate(dma_sheets) if "quotes" in s.lower() or "weekly" in s.lower()), 0)
            
            st.divider()
            
            c1, c2 = st.columns(2)
            with c1:
                quotes_sheet = st.selectbox("Sheet for Quotes/Actuals:", dma_sheets, index=quotes_default)
            with c2:
                total_sheet_name = st.selectbox("Sheet for Total Incremental:", total_sheets, index=0)
            
            st.caption("Note: 'Weekly Support 1' sheet is detected automatically from the DMA file.")

            if st.button("Load Model Keys & Types"):
                with st.spinner("Loading..."):
                    opts = dma_get_options(dma_file.getvalue(), quotes_sheet)
                    st.session_state.dma_model_keys = opts["available_model_keys"]
                    st.session_state.dma_types = opts["available_types"]
                    st.success(f"Loaded {len(opts['available_model_keys'])} keys.")
                    
            # Multiselects
            keys = st.session_state.dma_model_keys
            types = st.session_state.dma_types
            
            if keys:
                st.divider()
                sel_keys = st.multiselect("Select Model Keys:", keys, default=keys)
                sel_types = st.multiselect("Select Types:", types, default=types)
                
                if st.button("Generate Factors Workbook"):
                    placeholder = st.empty()
                    placeholder.markdown(amfam_loader("Generating Factors..."), unsafe_allow_html=True)
                    
                    try:
                        f_bytes, msg = dma_generate_factors(
                            dma_file.getvalue(), quotes_sheet,
                            total_file.getvalue(), total_sheet_name,
                            sel_keys, sel_types
                        )
                        
                        st.session_state.factor_bytes = f_bytes
                        st.session_state.factor_name = "model_factors_output.xlsx"
                        placeholder.empty()
                        st.success(msg)
                        
                        st.download_button(
                            "⬇️ Download model_factors_output.xlsx",
                            data=f_bytes,
                            file_name="model_factors_output.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    except Exception as e:
                        placeholder.empty()
                        st.error(f"Error: {str(e)}")
                        
        except Exception as e:
            st.error(f"Error reading files: {e}")
    else:
        st.info("Please upload both the DMA Summary file and the Total Incremental file to proceed.")

# ======================================================
# TAB 2: FACTOR PERIODS
# ======================================================
with tab_factor:
    st.header("Step 1: Process Factor File")
    
    # Added on_change=reset_all_state
    factor_up = st.file_uploader(
        "Upload Factor Workbook (or use generated)", 
        type=["xlsx", "xls"], 
        key="factor_up",
        on_change=reset_all_state
    )
    
    if factor_up:
        st.session_state.factor_bytes = factor_up.getvalue()
        st.session_state.factor_name = factor_up.name
        # Note: reset_all_state cleared factor_summary, so we regenerate below
        
    eff_bytes = st.session_state.factor_bytes
    
    if eff_bytes:
        if st.session_state.factor_summary is None:
            # Initial read to get periods
            _, _, _, summary = process_factor_file(eff_bytes)
            st.session_state.factor_summary = summary
            
        summ = st.session_state.factor_summary
        all_periods = summ.get("periods_available", [])
        
        st.write(f"**Loaded File:** {st.session_state.factor_name}")
        st.write(f"**Rows:** {summ.get('rows_before')}")
        
        removals = st.multiselect("Select Time Periods to REMOVE:", all_periods)
        
        if st.button("⚙️ Process & Remove Selected Periods"):
            placeholder = st.empty()
            placeholder.markdown(amfam_loader("Processing..."), unsafe_allow_html=True)
            
            try:
                processed_bytes, _, _, new_summ = process_factor_file(eff_bytes, removals)
                st.session_state.factor_processed_bytes = processed_bytes
                st.session_state.factor_processed_summary = new_summ
                # Update main bytes to processed version for next steps
                st.session_state.factor_bytes = processed_bytes 
                st.session_state.factor_summary = new_summ
                placeholder.empty()
                st.success("Factors processed and updated in memory.")
            except Exception as e:
                placeholder.empty()
                st.error(f"Error: {e}")

        if st.session_state.factor_processed_bytes:
             st.download_button(
                "⬇️ Download Factors_Processed.xlsx",
                data=st.session_state.factor_processed_bytes,
                file_name="Factors_Processed.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    else:
        st.info("Upload a file or generate one in Step 0.")

# ======================================================
# TAB 3: GRANULAR OVERRIDE
# ======================================================
with tab_granular:
    st.header("Step 2: Update Granular OVERRIDE")
    
    if not st.session_state.factor_bytes:
        st.warning("No factor file in memory. Go to Step 0 or 1.")
    else:
        # Granular upload doesn't strictly need reset_all_state unless you want to force re-run of this tab
        gran_file = st.file_uploader("Upload Granular Workbook", type=["xlsx", "xls"], key="gran_up")
        
        c1, c2 = st.columns(2)
        with c1: lower = st.number_input("Lower Tolerance", 0.0, 2.0, 0.95, 0.01)
        with c2: upper = st.number_input("Upper Tolerance", 0.0, 2.0, 1.05, 0.01)
        
        if st.button("🚀 Run Granular Update"):
            if not gran_file:
                st.error("Upload Granular file.")
            else:
                placeholder = st.empty()
                placeholder.markdown(amfam_loader("Updating Granular..."), unsafe_allow_html=True)
                
                try:
                    # Re-process factor file to get map
                    _, f_rows, f_map, _ = process_factor_file(st.session_state.factor_bytes)
                    
                    st.session_state.granular_bytes = gran_file.getvalue()
                    
                    g_bytes, log_bytes, summary, df_preview = update_granular_with_factors(
                        f_rows, f_map, gran_file.getvalue(), lower, upper
                    )
                    
                    st.session_state.updated_granular_bytes = g_bytes
                    st.session_state.granular_log_bytes = log_bytes
                    st.session_state.granular_preview = df_preview.head(10)
                    
                    placeholder.empty()
                    st.success(f"Done. Multiplied: {summary['multiplied_count']}, Skipped: {summary['skipped_count']}")
                except Exception as e:
                    placeholder.empty()
                    st.error(f"Error: {e}")
        
        if st.session_state.updated_granular_bytes:
            st.download_button("⬇️ Download Granular_Updated.xlsx", st.session_state.updated_granular_bytes, "Granular_Updated.xlsx")
            st.download_button("⬇️ Download Granular_Log.xlsx", st.session_state.granular_log_bytes, "Granular_Log.xlsx")
            st.dataframe(st.session_state.granular_preview)

# ======================================================
# TAB 4: ADS UPDATE
# ======================================================
with tab_ads:
    st.header("Step 3: Update ADS")
    
    ms_file = st.file_uploader("Upload Master Spec", type=["xlsx", "xls"], key="ms_up")
    ads_file = st.file_uploader("Upload ADS File", type=["xlsx", "xls", "csv"], key="ads_up")
    
    c1, c2 = st.columns(2)
    with c1: lower_a = st.number_input("Lower Tol (ADS)", 0.0, 2.0, 0.95, 0.01, key="la")
    with c2: upper_a = st.number_input("Upper Tol (ADS)", 0.0, 2.0, 1.05, 0.01, key="ua")
    
    if st.button("🚀 Run ADS Update"):
        if not (ms_file and ads_file and st.session_state.factor_bytes):
            st.error("Missing Files (Factors, Master Spec, or ADS).")
        else:
            placeholder = st.empty()
            placeholder.markdown(amfam_loader("Updating ADS..."), unsafe_allow_html=True)
            
            try:
                # Need factor rows
                _, f_rows, _, _ = process_factor_file(st.session_state.factor_bytes)
                
                # Need granular df for contrib skip logic
                if st.session_state.updated_granular_bytes:
                    g_df = pd.read_excel(BytesIO(st.session_state.updated_granular_bytes), sheet_name="OVERRIDE")
                elif st.session_state.granular_bytes:
                    g_df = pd.read_excel(BytesIO(st.session_state.granular_bytes), sheet_name="OVERRIDE")
                else:
                    g_df = None # Should warn user really, but logic handles None
                
                ads_is_csv = ads_file.name.lower().endswith(".csv")
                
                res_ads, res_log, summ = update_ads_with_factors(
                    ms_file.getvalue(), ads_file.getvalue(), ads_is_csv,
                    f_rows, g_df, lower_a, upper_a
                )
                
                st.session_state.ads_updated_bytes = res_ads
                st.session_state.ads_log_bytes = res_log
                
                placeholder.empty()
                st.success(f"Done. Applied: {summ['total_applied']}, Skipped: {summ['total_skipped']}")
            except Exception as e:
                placeholder.empty()
                st.error(f"Error: {e}")
                
    if st.session_state.ads_updated_bytes:
        ext = "csv" if ads_file.name.lower().endswith(".csv") else "xlsx"
        st.download_button(f"⬇️ Download ADS_Updated.{ext}", st.session_state.ads_updated_bytes, f"ADS_Updated.{ext}")
        st.download_button("⬇️ Download ADS_Log.xlsx", st.session_state.ads_log_bytes, "ADS_Log.xlsx")
