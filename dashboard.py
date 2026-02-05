import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import base64
from pathlib import Path
from datetime import timedelta
from streamlit_autorefresh import st_autorefresh


try:
    from data_fetcher import REGIONS, run_region_cached,run_region_cached_with_range,get_api_errors, clear_api_errors 
except ImportError:
    st.error("Could not import 'data_fetcher.py'. Please ensure the file exists and is named correctly.")
    st.stop()


st.set_page_config(page_title="Fuel Monitoring Dashboard", layout="wide")

st_autorefresh(interval= 4 * 60 * 60 *1000, key="auto_refresh")


def load_image_base64(image_path):
    try:
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except FileNotFoundError:
        return ""

LOGO_BASE64 = load_image_base64("assets/logo.png")


RATE_CATEGORIES = [
    "Refills ignored by humans",
    "Total refill alerts",
    "Refill True Positive rate (%)",
    "Refill False Positive rate (%)",
    "Thefts ignored by humans",
    "Total theft alerts",
    "Theft True Positive rate (%)",
    "Theft False Positive rate (%)"
]

header_col1, header_col2 = st.columns([6, 1])

with header_col1:
    st.title("Fuel Trends Dashboard")

with header_col2:
    st.markdown(
        f"""
        <div style="display:flex; justify-content:flex-end; align-items:center;">
            <img 
                src="data:image/png;base64,{LOGO_BASE64}"
                style="
                    height:70px;
                    max-width:260px;
                    width:auto;
                    image-rendering: -webkit-optimize-contrast;
                    image-rendering: crisp-edges;
                "
            />
        </div>
        """,
        unsafe_allow_html=True
    )

col1, col2 = st.columns(2)
end_time_start = pd.Timestamp.now() - pd.Timedelta(days=2)

with col1:
    START_DATE = st.date_input(
        "Start Date",
        value=(end_time_start - pd.Timedelta(days=10)).date()
    )

with col2:
    END_DATE = st.date_input(
        "End Date",
        value=end_time_start.date()
    )
def filter_data_by_date_range(results, start_ms, end_ms):

    filtered = {}
    
    for region, data in results.items():
        filtered[region] = {}
        
        for key, df in data.items():
            if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                filtered[region][key] = df
                continue
                

            if isinstance(df, pd.DataFrame) and 'time_ms' in df.columns:
                filtered[region][key] = df[
                    (df['time_ms'] >= start_ms) & 
                    (df['time_ms'] <= end_ms)
                ].copy()
            else:
                
                filtered[region][key] = df
                
    return filtered


start_time_ms = int(pd.Timestamp(START_DATE).normalize().timestamp() * 1000)

end_time_ms = int((pd.Timestamp(END_DATE).normalize() + pd.Timedelta(days=1)).timestamp() * 1000)

if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

clear_api_errors()

@st.cache_data(show_spinner=True, ttl=6 * 60 * 60)
def load_all_regions(start_ms, end_ms):

    from data_fetcher import run_region_cached_with_range
    return {
        region: run_region_cached_with_range(region, url, start_ms, end_ms)
        for region, url in REGIONS.items()
    }

with st.spinner("Fetching data from Dashboard APIs..."):
    RESULTS = load_all_regions(start_time_ms, end_time_ms)
    

    api_errors = get_api_errors()
    
    if api_errors:
        st.warning(
            f"**Data Incomplete Due to API Errors**\n\n"
            f"**{len(api_errors)} API requests failed** due to timeouts. "
            f"Some data may be missing in the dashboard."
        )
@st.cache_data(show_spinner=True, ttl=6 * 60 * 60)
def load_all_regions(start_ms, end_ms):

    from data_fetcher import run_region_cached_with_range
    return {
        region: run_region_cached_with_range(region, url, start_ms, end_ms)
        for region, url in REGIONS.items()
    }

with st.spinner("Fetching data from Dashboard APIs..."):
    RESULTS = load_all_regions(start_time_ms, end_time_ms)

def build_fuel_summary_values(fill_daily, theft_daily):


    total_theft = theft_daily["amount"].sum() if not theft_daily.empty else 0
    total_refill = fill_daily["amount"].sum() if not fill_daily.empty else 0

    mv_avg_theft = (
        int(theft_daily["moving average"].iloc[-1])
        if not theft_daily.empty else 0
    )

    mv_avg_refill = (
        int(fill_daily["moving average"].iloc[-1])
        if not fill_daily.empty else 0
    )

    return [
        round(total_theft, 2),
        round(total_refill, 2),
        mv_avg_theft,
        mv_avg_refill
    ]
def build_lng_cng_ratio(fill_raw):


    if fill_raw.empty or "fuel_type" not in fill_raw.columns:
        return None, None

    def calc_ratio(df):
        if df.empty:
            return None
        total = df["amount"].count()
        kgs = df["Amount_kgs"].count() if "Amount_kgs" in df.columns else 0
        return round((kgs / total) * 100, 2) if total else None

    lng_df = fill_raw[fill_raw["fuel_type"].str.lower() == "lng"]
    cng_df = fill_raw[fill_raw["fuel_type"].str.lower() == "cng"]

    return calc_ratio(lng_df), calc_ratio(cng_df)


def build_tp_fp_table(
    refill_df,
    theft_df,
    ignored_refill_df,
    ignored_theft_df
):


    total_refills = len(refill_df)
    total_thefts = len(theft_df)

    ignored_refills = len(ignored_refill_df)
    ignored_thefts = len(ignored_theft_df)

    tp_refill = (
        (total_refills - ignored_refills) / total_refills * 100
        if total_refills else 0
    )

    fp_refill = (
        ignored_refills / total_refills * 100
        if total_refills else 0
    )

    tp_theft = (
        (total_thefts - ignored_thefts) / total_thefts * 100
        if total_thefts else 0
    )

    fp_theft = (
        ignored_thefts / total_thefts * 100
        if total_thefts else 0
    )

    return [
        ignored_refills,
        total_refills,
        round(tp_refill, 2),
        round(fp_refill, 2),
        ignored_thefts,
        total_thefts,
        round(tp_theft, 2),
        round(fp_theft, 2)
    ]
def create_plot(df, title, unit):
    fig = go.Figure()

    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    fig.add_trace(go.Scatter(
        x=df["time"], y=df["amount"],
        mode="markers+lines", name="Amount",
        line=dict(width=4),  # Increased line thickness
        marker=dict(size=14)
    ))

    if "moving average" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["time"],
            y=df["moving average"],
            mode="lines",
            name="Moving Avg",
            line=dict(
                dash="dot",
                color="green",     
                width=2
            )
        ))

    total = df["amount"].sum()
    avg = total / len(df) if len(df) else 0

    y_max = max(
        df["amount"].max(),
        df["moving average"].max() if "moving average" in df.columns else 0
    ) * 1.3

    fig.update_layout(
        title={
            "text": (
                f"<b style='font-size:30px'>{title}</b><br>"
                f"<span style='font-size:26px'>"
                f"Total: {total:.2f} | Avg/Day: {avg:.2f}"
                f"</span>"
            ),
            "x": 0.5,
            "xanchor": "center"
        },
        xaxis_title="Date",
        yaxis_title=unit,
        xaxis=dict(
            title_font=dict(size=26, color='black', family='Arial Black'),
            tickfont=dict(size=17, color='black', family='Arial Black'),
            showgrid=True,
            gridcolor='lightgray',
            tickmode='linear',
            dtick=86400000,  
            tickformat='%b %d\n%Y',  
            tickangle=-45
        ),
        yaxis=dict(
            range=[0, y_max],
            title_font=dict(size=26, color='black', family='Arial Black'),
            tickfont=dict(size=17, color='black', family='Arial Black'),
            showgrid=True,
            gridcolor='lightgray'
        ),
        height=450,
        margin=dict(t=100, b=40, l=60, r=60),
        showlegend=False
    )

    return fig


def create_plot_usfs(df, title, unit):
    fig = go.Figure()

    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    fig.add_trace(go.Scatter(
        x=df["time"], y=df["amount"],
        mode="markers+lines", name="Amount"
        ,line=dict(width=4),  # Increased line thickness
        marker=dict(size=14)
    ))

    if "moving average" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["time"],
            y=df["moving average"],
            mode="lines",
            name="Moving Avg",
            line=dict(
                dash="dot",
                color="green",     
                width=2
            )
        ))

    total = df["amount"].sum()
    avg = total / len(df) if len(df) else 0

    y_max = max(
        df["amount"].max(),
        df["moving average"].max() if "moving average" in df.columns else 0
    ) * 1.3

    fig.update_layout(
        title={
            "text": (
                f"<b style='font-size:30px'>{title}</b><br>"
                f"<span style='font-size:26px'>"
                f"Total: {total:.2f} | Avg/Day: {avg:.2f}"
                f"</span>"
            ),
            "x": 0.5,
            "xanchor": "center"
        },
        xaxis_title="Date",
        yaxis_title=unit,
        xaxis=dict(
            title_font=dict(size=26, color='black', family='Arial Black'),
            tickfont=dict(size=17, color='black', family='Arial Black'),
            showgrid=True,
            gridcolor='lightgray',
            tickmode='linear',
            dtick=86400000,  
            tickformat='%b %d\n%Y',  
            tickangle=-45
        ),
        yaxis=dict(
            range=[0, y_max],
            title_font=dict(size=26, color='black', family='Arial Black'),
            tickfont=dict(size=17, color='black', family='Arial Black'),
            showgrid=True,
            gridcolor='lightgray'
        ),
        height=420,
        margin=dict(t=100, b=40, l=60, r=60),
        showlegend=False
    )

    return fig


def create_plot_low_fuel(df, title):
    fig = go.Figure()

    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    fig.add_trace(go.Scatter(
        x=df["time"],
        y=df["vehicle_id"],
        mode="markers+lines",
        name="Alert Count",
        line=dict(width=4),  # Increased line thickness
        marker=dict(size=14)
    ))

    if "moving average" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["time"],
            y=df["moving average"],
            mode="lines",
            name="Moving Avg",
            line=dict(dash="dot", color="red", width=4)
        ))

    total = df["vehicle_id"].sum()
    avg = total / len(df) if len(df) else 0

    y_max = max(
        df["vehicle_id"].max(),
        df["moving average"].max() if "moving average" in df.columns else 0
    ) * 1.3

    fig.update_layout(
        title={
            "text": (
                f"<b style='font-size:30px'>{title}</b><br>"
                f"<span style='font-size:26px'>"
                f"Total: {total:.2f} | Avg/Day: {avg:.2f}"
                f"</span>"
            ),
            "x": 0.5,
            "xanchor": "center"
        },
        xaxis_title="Date",
        yaxis_title="Alert Count",
        xaxis=dict(
            title_font=dict(size=26, color='black', family='Arial Black'),
            tickfont=dict(size=17, color='black', family='Arial Black'),
            showgrid=True,
            gridcolor='lightgray',
            tickmode='linear',
            dtick=86400000,  
            tickformat='%b %d\n%Y', 
            tickangle=-45
        ),
        yaxis=dict(
            range=[0, y_max],
            title_font=dict(size=26, color='black', family='Arial Black'),
            tickfont=dict(size=17, color='black', family='Arial Black'),
            showgrid=True,
            gridcolor='lightgray'
        ),
        height=420,
        margin=dict(t=100, b=40, l=60, r=60),
        showlegend=False
    )

    return fig


def create_plot_pv(df, title, unit):
    fig = go.Figure()

    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    fig.add_trace(go.Scatter(
        x=df["time"],
        y=df["probable_variation_max"],
        mode="markers+lines",
        name="Probable Variation",
        line=dict(width=4),  # Increased line thickness
        marker=dict(size=14)
    ))

    if "moving average" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["time"],
            y=df["moving average"],
            mode="lines",
            name="Moving Avg",
            line=dict(
                dash="dot",
                color="green",     
                width=2
            )
    ))

    total = df["probable_variation_max"].sum()
    avg = total / len(df) if len(df) else 0

    y_max = max(
        df["probable_variation_max"].max(),
        df["moving average"].max() if "moving average" in df.columns else 0
    ) * 1.3

    fig.update_layout(
        title={
            "text": (
                f"<b style='font-size:30px'>{title}</b><br>"
                f"<span style='font-size:26px'>"
                f"Total: {total:.2f} | Avg/Day: {avg:.2f}"
                f"</span>"
            ),
            "x": 0.5,
            "xanchor": "center"
        },
        xaxis_title="Date",
        yaxis_title=unit,
        xaxis=dict(
            title_font=dict(size=26, color='black', family='Arial Black'),
            tickfont=dict(size=17, color='black', family='Arial Black'),
            showgrid=True,
            gridcolor='lightgray',
            tickmode='linear',
            dtick=86400000,  
            tickformat='%b %d\n%Y',  
            tickangle=-45
        ),
        yaxis=dict(
            range=[0, y_max],
            title_font=dict(size=26, color='black', family='Arial Black'),
            tickfont=dict(size=17, color='black', family='Arial Black'),
            showgrid=True,
            gridcolor='lightgray'
        ),
        height=420,
        margin=dict(t=100, b=40, l=60, r=60),
        showlegend=False
    )

    return fig



tabs = st.tabs([
    "INDIA",
    "NASA",
    "EU",
    "FML",
    "FUEL SUMMARY",
    "DATA LOSS",
    "MAIN DASHBOARD",
    "EXPORT DATA" 
])

UNIT_MAP = {
    "IND": "Liters",
    "NASA": "Gallons",
    "EU": "Liters",
    "FML": "Liters"
}
def build_combined_data_loss_summary(results):
    rows = []

    for region, data in results.items():
        summary = data.get("data_loss_summary", pd.DataFrame())

        if summary is not None and not summary.empty:
            tmp = summary.copy()
            tmp["Region"] = region
            rows.append(tmp)

    if not rows:
        return pd.DataFrame()

    final_df = pd.concat(rows, ignore_index=True)
    final_df = final_df[["Region", "Data loss type", "Count"]]

    return final_df

for tab, region in zip(tabs[:4], REGIONS.keys()):
    with tab:
        st.header(f"{region} Region Analysis")
        
        
        st.markdown(
            """
            <div style="
                background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
                padding: 15px 30px;
                border-radius: 10px;
                margin-bottom: 20px;
                text-align: center;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            ">
                <span style="font-size: 18px; font-weight: bold; color: #333; margin-right: 30px;">
                    Legend:
                </span>
                <span style="font-size: 19px; color: #1e88e5; margin-right: 25px;">
                    <span style="display: inline-block; width: 40px; height: 3px; background: #1e88e5; vertical-align: middle; margin-right: 8px;"></span>
                    <strong>Amount / Alert Count</strong>
                </span>
                <span style="font-size: 19px; color: #43a047;">
                    <span style="display: inline-block; width: 40px; height: 3px; background: #43a047; border-top: 2px dotted #43a047; vertical-align: middle; margin-right: 8px;"></span>
                    <strong>Moving Average</strong>
                </span>
            </div>
            """,
            unsafe_allow_html=True
        )

        unit = UNIT_MAP[region]


        fill_daily = RESULTS[region]["fill_daily"]
        theft_daily = RESULTS[region]["theft_daily"]
        

        fill_cev_daily = RESULTS[region]["fill_cev_daily"]
        theft_cev_daily = RESULTS[region]["theft_cev_daily"]
        
        fill_usfs = RESULTS[region]["fill_usfs_daily"]
        theft_usfs = RESULTS[region]["theft_usfs_daily"]
        fill_pv = RESULTS[region]["fill_pv_daily"]
        theft_pv = RESULTS[region]["theft_pv_daily"]
        low_fuel_daily = RESULTS[region]["low_fuel_daily"]


        st.subheader("Fuel Refill (DPL)")
        if not fill_daily.empty:
            st.plotly_chart(create_plot(fill_daily, f"{region} Refill (DPL)", unit), True)
        else:
            st.info("No refill data")

        st.subheader("Fuel Theft (DPL)")
        if not theft_daily.empty:
            st.plotly_chart(create_plot(theft_daily, f"{region} Theft (DPL)", unit), True)
        else:
            st.info("No theft data")


        st.markdown("---")
        st.subheader("Fuel Refill (CEV/Off-Highway)")
        if not fill_cev_daily.empty:
            st.plotly_chart(create_plot(fill_cev_daily, f"{region} Refill (CEV)", unit), True)
        else:
            st.info("No CEV refill data")

        st.subheader("Fuel Theft (CEV/Off-Highway)")
        if not theft_cev_daily.empty:
            st.plotly_chart(create_plot(theft_cev_daily, f"{region} Theft (CEV)", unit), True)
        else:
            st.info("No CEV theft data")


        st.markdown("---")
        st.subheader("USFS Refill")
        if not fill_usfs.empty:
            st.plotly_chart(create_plot_usfs(fill_usfs, f"{region} USFS Refill", unit), True)
        else:
            st.info("No USFS refill data")

        st.subheader("USFS Theft")
        if not theft_usfs.empty:
            st.plotly_chart(create_plot_usfs(theft_usfs, f"{region} USFS Theft", unit), True)
        else:
            st.info("No USFS theft data")


        st.markdown("---")
        st.subheader("Probable Variation – Refill")
        if not fill_pv.empty:
            st.plotly_chart(create_plot_pv(fill_pv, f"{region} PV Refill", unit), True)
        else:
            st.info("No PV refill data")

        st.subheader("Probable Variation – Theft")
        if not theft_pv.empty:
            st.plotly_chart(create_plot_pv(theft_pv, f"{region} PV Theft", unit), True)
        else:
            st.info("No PV theft data")


        st.markdown("---")
        st.subheader("Low Fuel Level Alerts")
        if not low_fuel_daily.empty:
            st.plotly_chart(
                create_plot_low_fuel(low_fuel_daily, f"{region} Low Fuel Alerts"),
                use_container_width=True
            )
        else:
            st.info("No low fuel alerts")

with tabs[5]:
    st.markdown(
        "<h2 style='text-align:center;'> Data Loss Summary</h2>",
        unsafe_allow_html=True
    )

    for region in REGIONS.keys():
        summary = RESULTS[region].get("data_loss_summary", pd.DataFrame())

        st.markdown(
            f"<h4 style='text-align:center;'> {region} Region</h4>",
            unsafe_allow_html=True
        )

        if not summary.empty:
            st.dataframe(
                summary,
                use_container_width=True,
                hide_index=True  #  no index
            )
        else:
            st.info(f"No data loss events for {region}")

        st.markdown("---")

with tabs[4]:
    st.markdown(
        "<h2 style='text-align:center;'> Fuel Summary</h2>",
        unsafe_allow_html=True
    )

    st.markdown("---")

    for region in ["IND", "NASA","EU","FML"]:
        st.markdown(
            f"<h4 style='text-align:center;'> {region}",
            unsafe_allow_html=True
        )

        data = RESULTS[region]
        dpl_values = build_fuel_summary_values(
            fill_daily=data["fill_daily"],
            theft_daily=data["theft_daily"]
        )


        cev_values = build_fuel_summary_values(
            fill_daily=data["fill_cev_daily"],
            theft_daily=data["theft_cev_daily"]
        )

        summary_df = pd.DataFrame({
            "Category": [
                f"Total Theft {region}",
                f"Total Refill {region}",
                f"Moving Average Theft {region}",
                f"Moving Average Fillings {region}"
            ],
            "DPL": dpl_values,
            "OFF HIGHWAY": cev_values
        })

        st.dataframe(
            summary_df,
            use_container_width=True,
            hide_index=True
        )


        lng_ratio, cng_ratio = build_lng_cng_ratio(data["fill_raw"])

        ratio_lines = []

        if lng_ratio is not None:
            ratio_lines.append(
                f"• Ratio of refill captured in kgs with respect to liters for LNG: "
                f"<b>{lng_ratio:.2f}%</b>"
            )

        if cng_ratio is not None:
            ratio_lines.append(
                f"• Ratio of refill captured in kgs with respect to liters for CNG: "
                f"<b>{cng_ratio:.2f}%</b>"
            )

        if ratio_lines:
            st.markdown(
                "<div style='padding:10px 0;'>" + "<br>".join(ratio_lines) + "</div>",
                unsafe_allow_html=True
            )

        st.markdown("---")

        dpl_values = build_tp_fp_table(
            refill_df=data["fill_raw"],
            theft_df=data["theft_raw"],
            ignored_refill_df=data["fill_raw"][data["fill_raw"]["alert_fuel_filling_ignore"] == True]
            if "alert_fuel_filling_ignore" in data["fill_raw"] else pd.DataFrame(),
            ignored_theft_df=data["theft_raw"][data["theft_raw"]["alert_fuel_theft_ignore"] == True]
            if "alert_fuel_theft_ignore" in data["theft_raw"] else pd.DataFrame()
        )


        cev_values = build_tp_fp_table(
            refill_df=data["fill_cev"],
            theft_df=data["theft_cev"],
            ignored_refill_df=data["fill_cev"][data["fill_cev"]["alert_fuel_filling_ignore"] == True]
            if "alert_fuel_filling_ignore" in data["fill_cev"] and not data["fill_cev"].empty else pd.DataFrame(),
            ignored_theft_df=data["theft_cev"][data["theft_cev"]["alert_fuel_theft_ignore"] == True]
            if "alert_fuel_theft_ignore" in data["theft_cev"] and not data["theft_cev"].empty else pd.DataFrame()
        )

        rate_df = pd.DataFrame({
            "Metric": RATE_CATEGORIES,
            "DPL": dpl_values,
            "OFF HIGHWAY": cev_values
        })

        st.dataframe(
            rate_df,
            use_container_width=True,
            hide_index=True
        )

        st.markdown("---")
#-------

# MAIN DASHBOARD tab

ROTATION_MINUTES = 1 

with tabs[6]:  
    from datetime import datetime
    import time
    

    if 'rotation_start_time' not in st.session_state:
        st.session_state.rotation_start_time = time.time()
    if 'last_region' not in st.session_state:
        st.session_state.last_region = None
    if 'show_loading' not in st.session_state:
        st.session_state.show_loading = True
    

    elapsed_seconds = time.time() - st.session_state.rotation_start_time
    rotation_seconds = ROTATION_MINUTES * 60
    cycle_position = int(elapsed_seconds // rotation_seconds) % 5  
    

    REGION_ORDER = ["IND", "NASA", "EU", "FML", "LOW_FUEL"]
    current_region = REGION_ORDER[cycle_position]
    

    if st.session_state.last_region != current_region:
        st.session_state.show_loading = True
    
    st.session_state.last_region = current_region
    

    st_autorefresh(interval=ROTATION_MINUTES * 60 * 1000, key="region_auto_rotate")
    
    st.markdown(
        """
        <style>
        /* Smooth fade-in and slide animation */
        @keyframes fadeInSlide {
            0% { 
                opacity: 0; 
                transform: translateY(30px) scale(0.98);
            }
            100% { 
                opacity: 1; 
                transform: translateY(0) scale(1);
            }
        }
        
        @keyframes fadeOut {
            0% { 
                opacity: 1; 
                transform: scale(1);
            }
            100% { 
                opacity: 0; 
                transform: scale(0.95);
            }
        }
        
        /* Loading spinner animation */
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        
        @keyframes loadingFadeOut {
            0% { opacity: 1; }
            100% { opacity: 0; pointer-events: none; }
        }
        
        .dashboard-container {
            animation: fadeInSlide 1.5s cubic-bezier(0.4, 0, 0.2, 1);
        }
        
        .dashboard-container.fade-out {
            animation: fadeOut 0.8s cubic-bezier(0.4, 0, 0.2, 1);
        }
        
        /* Hide Streamlit elements for TV */
        #MainMenu, footer, header, .stDeployButton {
            visibility: hidden !important;
            display: none !important;
        }
        
        /* Hide all top controls (tabs, date inputs, refresh button) */
        [data-testid="stHorizontalBlock"]:first-of-type,
        .stTabs,
        [data-testid="column"]:has(> div > div > label),
        button:contains("Refresh Data") {
            display: none !important;
        }
        
        /* Force fullscreen mode */
        .main {
            padding: 0 !important;
        }
        
        /* Optimize for TV display */
        .main .block-container {
            padding: 0rem !important;
            max-width: 100% !important;
        }
        
        /* Make all content fill screen */
        section[data-testid="stAppViewContainer"] {
            padding: 0 !important;
        }
        
        /* Remove top padding */
        .appview-container {
            padding-top: 0 !important;
        }
        
        /* Smooth chart transitions */
        .js-plotly-plot {
            transition: all 0.8s cubic-bezier(0.4, 0, 0.2, 1);
        }
        
        /* Enhanced region header - smaller for TV */
        .region-header {
            animation: fadeInSlide 1s cubic-bezier(0.4, 0, 0.2, 1);
            text-align: center;
            padding: 15px 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 10px;
            margin-bottom: 15px;
            box-shadow: 0 8px 30px rgba(102, 126, 234, 0.3);
            position: relative;
            overflow: hidden;
        }
        
        /* Loading overlay */
        .loading-overlay {
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(102, 126, 234, 0.95);
            backdrop-filter: blur(5px);
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            animation: loadingFadeOut 0.8s ease-out 1.2s forwards;
            z-index: 10;
        }
        
        .loading-spinner {
            width: 50px;
            height: 50px;
            border: 4px solid rgba(255, 255, 255, 0.3);
            border-top: 4px solid white;
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin-bottom: 15px;
        }
        
        .loading-text {
            color: white;
            font-size: 20px;
            font-weight: bold;
            animation: pulse 1.5s ease-in-out infinite;
        }
        
        .region-title {
            color: white;
            font-size: 42px;
            margin: 0;
            font-weight: bold;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
            animation: pulse 2s ease-in-out;
        }
        
        .region-subtitle {
            color: #e0e0e0;
            font-size: 18px;
            margin: 8px 0;
        }
        
        /* Enhanced progress bar with glow */
        .progress-container {
            width: 100%;
            height: 5px;
            background: rgba(255,255,255,0.2);
            border-radius: 3px;
            margin-top: 10px;
            overflow: hidden;
            box-shadow: 0 0 10px rgba(79, 172, 254, 0.3);
        }
        
        .progress-bar {
            height: 100%;
            background: linear-gradient(90deg, #4facfe 0%, #00f2fe 100%);
            border-radius: 3px;
            box-shadow: 0 0 15px rgba(79, 172, 254, 0.8);
            animation-name: progress-animation;
            animation-timing-function: linear;
            animation-fill-mode: forwards;
        }
        
        @keyframes progress-animation {
            0% { width: 0%; }
            100% { width: 100%; }
        }
        
        /* Chart row animation with stagger effect */
        .chart-row {
            animation: fadeInSlide 1.5s cubic-bezier(0.4, 0, 0.2, 1);
        }
        
        .chart-row:nth-child(1) { animation-delay: 0.1s; }
        .chart-row:nth-child(2) { animation-delay: 0.2s; }
        .chart-row:nth-child(3) { animation-delay: 0.3s; }
        .chart-row:nth-child(4) { animation-delay: 0.4s; }
        
        /* Individual chart animation */
        .stPlotlyChart {
            animation: fadeInSlide 1.2s cubic-bezier(0.4, 0, 0.2, 1);
        }
        
        /* Countdown timer styling */
        .countdown-timer {
            font-weight: bold;
            color: #4facfe;
            text-shadow: 0 0 10px rgba(79, 172, 254, 0.5);
        }
        
        /* Compact legend for TV */
        .tv-legend {
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            padding: 10px 20px;
            border-radius: 8px;
            margin-bottom: 10px;
            text-align: center;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        /* Warning banner styling */
        .data-warning {
            background: linear-gradient(135deg, #fff3cd 0%, #ffc107 50%, #ff9800 100%);
            padding: 12px 20px;
            border-radius: 8px;
            margin-bottom: 15px;
            text-align: center;
            box-shadow: 0 4px 12px rgba(255, 152, 0, 0.3);
            border-left: 5px solid #ff6f00;
            animation: pulse 2s ease-in-out infinite;
        }
        
        .warning-icon {
            font-size: 24px;
            margin-right: 10px;
            vertical-align: middle;
        }
        
        .warning-text {
            color: #663c00;
            font-size: 18px;
            font-weight: bold;
            display: inline-block;
            vertical-align: middle;
        }
        </style>
        """,
        unsafe_allow_html=True
    )
    
    rotation_duration = ROTATION_MINUTES * 60
    st.markdown(
        f"""
        <script>
        (function() {{
            const ROTATION_SECONDS = {rotation_duration};
            
            function updateCountdown() {{
                const now = new Date();
                const minutesSinceMidnight = now.getHours() * 60 + now.getMinutes();
                const secondsSinceMidnight = minutesSinceMidnight * 60 + now.getSeconds();
                
                const secondsInCurrentCycle = secondsSinceMidnight % ROTATION_SECONDS;
                const secondsRemaining = ROTATION_SECONDS - secondsInCurrentCycle;
                
                const minutesRemaining = Math.floor(secondsRemaining / 60);
                const secondsRemainingInMinute = secondsRemaining % 60;
                
                const countdownElement = document.querySelector('.countdown-timer');
                if (countdownElement) {{
                    countdownElement.textContent = minutesRemaining + ':' + secondsRemainingInMinute.toString().padStart(2, '0');
                }}
                
                if (secondsRemaining <= 2) {{
                    const container = document.querySelector('.dashboard-container');
                    if (container) {{
                        container.classList.add('fade-out');
                    }}
                }}
            }}
            
            setInterval(updateCountdown, 1000);
            updateCountdown();
        }})();
        </script>
        """,
        unsafe_allow_html=True
    )
    

    next_region = REGION_ORDER[(cycle_position + 1) % 5]
    
    region_display_names = {
        "IND": "INDIA",
        "NASA": "NASA",
        "EU": "EUROPE",
        "FML": "FML",
        "LOW_FUEL": "Low Fuel Alerts"
    }
    
    next_region_display = region_display_names.get(next_region, next_region)
    

    elapsed_in_cycle = elapsed_seconds % rotation_seconds
    remaining_in_cycle = rotation_seconds - elapsed_in_cycle
    

    if st.session_state.show_loading:
        loading_div = '<div class="loading-overlay"><div class="loading-spinner"></div><div class="loading-text">Loading Region Data...</div></div>'
    else:
        loading_div = ''
    

    progress_duration = ROTATION_MINUTES * 60

    progress_key = f"progress-{current_region}-{cycle_position}"
    

    if current_region == "LOW_FUEL":
        region_title = "LOW FUEL LEVEL ALERTS"
    else:
        region_title = current_region
    
    st.markdown(
        '<div class="region-header">' + 
        loading_div + 
        f'<h1 class="region-title">{region_title}</h1>' +
        f'<p class="region-subtitle">Next: <strong>{next_region_display}</strong> <span style="margin-left: 15px;"> <span class="countdown-timer">Calculating...</span></span></p>' +
        f'<div class="progress-container"><div class="progress-bar {progress_key}" style="animation-duration: {progress_duration}s;"></div></div>' +
        '</div>',
        unsafe_allow_html=True
    )
    

    if st.session_state.show_loading:
        st.session_state.show_loading = False
    

    st.markdown('<div class="dashboard-container">', unsafe_allow_html=True)
    

    if current_region == "LOW_FUEL":

        st.markdown(
            """
            <div class="tv-legend">
                <span style="font-size: 19px; font-weight: bold; color: #333; margin-right: 25px;">
                    Chart Legend:
                </span>
                <span style="font-size: 18px; color: #1e88e5; margin-right: 20px;">
                    <span style="display: inline-block; width: 35px; height:6px; background: #1e88e5; vertical-align: middle; margin-right: 6px;"></span>
                    <strong>Alert Count</strong>
                </span>
                <span style="font-size: 18px; color: #d32f2f;">
                    <span style="display: inline-block; width: 35px; height: 6px; background: #d32f2f; border-top: 2px dotted #d32f2f; vertical-align: middle; margin-right: 6px;"></span>
                    <strong>Moving Average</strong>
                </span>
            </div>
            """,
            unsafe_allow_html=True
        )
        

        low_fuel_charts = []
        missing_regions = []
        
        for region_key in ["IND", "NASA", "EU", "FML"]:
            if region_key in RESULTS: 
                low_fuel_data = RESULTS[region_key].get("low_fuel_daily", pd.DataFrame())
                if not low_fuel_data.empty:
                    low_fuel_charts.append((region_key, low_fuel_data))
                else:
                    missing_regions.append(region_display_names.get(region_key, region_key))
        
        # Show warning if any regions have missing data
        if missing_regions:
            warning_text = f"⚠️ No Low Fuel Alert data available for: {', '.join(missing_regions)}"
            st.markdown(
                f"""
                <div class="data-warning">
                    <span class="warning-icon">⚠️</span>
                    <span class="warning-text">{warning_text}</span>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        if len(low_fuel_charts) == 0:
            st.info("No low fuel alerts available for any region")
        elif len(low_fuel_charts) == 1:
            # Single chart - use full vertical space (considering header ~200px, legend ~60px, margins ~100px)
            region_key, low_fuel_data = low_fuel_charts[0]
            fig = create_plot_low_fuel(low_fuel_data, f"{region_key} Low Fuel Alerts")
            fig.update_layout(height=820)  # ~1080px screen - 260px overhead
            st.plotly_chart(fig, use_container_width=True, key=f"lowfuel_{region_key}")
        elif len(low_fuel_charts) == 2:
            # 2 charts side by side - use full vertical space
            col1, col2 = st.columns(2)
            
            region_key1, low_fuel_data1 = low_fuel_charts[0]
            with col1:
                fig = create_plot_low_fuel(low_fuel_data1, f"{region_key1} Low Fuel Alerts")
                fig.update_layout(height=820)  # Same height as single chart
                st.plotly_chart(fig, use_container_width=True, key=f"lowfuel_{region_key1}")
            
            region_key2, low_fuel_data2 = low_fuel_charts[1]
            with col2:
                fig = create_plot_low_fuel(low_fuel_data2, f"{region_key2} Low Fuel Alerts")
                fig.update_layout(height=820)
                st.plotly_chart(fig, use_container_width=True, key=f"lowfuel_{region_key2}")
        else:
            # 3 or 4 charts - arrange in 2 rows
            num_charts = len(low_fuel_charts)
            num_rows = (num_charts + 1) // 2
            
            # Calculate height for 2 rows to fit screen (considering legend, margins, spacing between rows ~80px)
            chart_height = 370  # (820 - 80) / 2 = 370px per chart
            
            chart_index = 0
            for row in range(num_rows):
                st.markdown(f'<div class="chart-row" style="animation-delay: {row * 0.1}s;">', unsafe_allow_html=True)
                
                remaining_charts = num_charts - chart_index
                num_cols = min(2, remaining_charts)
                
                if num_cols == 1:
                    # Last odd chart - keep same height for consistency
                    region_key, low_fuel_data = low_fuel_charts[chart_index]
                    fig = create_plot_low_fuel(low_fuel_data, f"{region_key} Low Fuel Alerts")
                    fig.update_layout(height=chart_height)
                    st.plotly_chart(fig, use_container_width=True, key=f"lowfuel_{region_key}")
                    chart_index += 1
                else:
                    # 2 charts in a row
                    cols = st.columns(2)
                    
                    for col_idx, col in enumerate(cols):
                        if chart_index < num_charts:
                            region_key, low_fuel_data = low_fuel_charts[chart_index]
                            with col:
                                fig = create_plot_low_fuel(low_fuel_data, f"{region_key} Low Fuel Alerts")
                                fig.update_layout(height=chart_height)
                                st.plotly_chart(fig, use_container_width=True, key=f"lowfuel_{region_key}")
                            chart_index += 1
                
                st.markdown('</div>', unsafe_allow_html=True)
    
    else:

        st.markdown(
            """
            <div class="tv-legend">
                <span style="font-size: 19px; font-weight: bold; color: #333; margin-right: 25px;">
                    Chart Legend:
                </span>
                <span style="font-size: 17px; color: #1e88e5; margin-right: 20px;">
                    <span style="display: inline-block; width: 35px; height: 3px; background: #1e88e5; vertical-align: middle; margin-right: 6px;"></span>
                    <strong>Amount / Count</strong>
                </span>
                <span style="font-size: 17px; color: #43a047;">
                    <span style="display: inline-block; width: 35px; height: 3px; background: #43a047; border-top: 2px dotted #43a047; vertical-align: middle; margin-right: 6px;"></span>
                    <strong>Moving Average</strong>
                </span>
            </div>
            """,
            unsafe_allow_html=True
        )
        
        unit = UNIT_MAP[current_region]
        data = RESULTS[current_region]
        
        # Check for missing data and build warning message
        missing_data_types = []
        
        if data["fill_daily"].empty:
            missing_data_types.append("DPL Refill")
        if data["theft_daily"].empty:
            missing_data_types.append("DPL Theft")
        if data["fill_cev_daily"].empty:
            missing_data_types.append("CEV Refill")
        if data["theft_cev_daily"].empty:
            missing_data_types.append("CEV Theft")
        if data["fill_pv_daily"].empty:
            missing_data_types.append("PV Refill")
        if data["theft_pv_daily"].empty:
            missing_data_types.append("PV Theft")
        if data["fill_usfs_daily"].empty:
            missing_data_types.append("USFS Refill")
        if data["theft_usfs_daily"].empty:
            missing_data_types.append("USFS Theft")
        
        # Display warning if there's missing data
        if missing_data_types:
            region_display = region_display_names.get(current_region, current_region)
            warning_text = f"⚠️ No data available for {region_display}: {', '.join(missing_data_types)}"
            st.markdown(
                f"""
                <div class="data-warning">
                    <span class="warning-icon">⚠️</span>
                    <span class="warning-text">{warning_text}</span>
                </div>
                """,
                unsafe_allow_html=True
            )
        

        available_charts = []
        

        if not data["fill_daily"].empty:
            available_charts.append(("fill_daily", create_plot))
        

        if not data["fill_cev_daily"].empty:
            available_charts.append(("fill_cev_daily", create_plot))
        

        if not data["theft_daily"].empty:
            available_charts.append(("theft_daily", create_plot))
        

        if not data["theft_cev_daily"].empty:
            available_charts.append(("theft_cev_daily", create_plot))
        

        if not data["fill_pv_daily"].empty:
            available_charts.append(("fill_pv_daily", create_plot_pv))
        

        if not data["theft_pv_daily"].empty:
            available_charts.append(("theft_pv_daily", create_plot_pv))
        

        if not data["fill_usfs_daily"].empty:
            available_charts.append(("fill_usfs_daily", create_plot_usfs))
        

        if not data["theft_usfs_daily"].empty:
            available_charts.append(("theft_usfs_daily", create_plot_usfs))
        

        num_charts = len(available_charts)
        
        if num_charts == 0:
            st.info(f"No data available for {region_display_names.get(current_region, current_region)} region")
        elif num_charts == 1:
            # Single chart - use full vertical space
            data_key, plot_func = available_charts[0]
            title = data_key.replace("_", " ").title().replace("Fill", "Refill")
            fig = plot_func(data[data_key], title, unit)
            fig.update_layout(height=820)  # Full screen height
            st.plotly_chart(fig, use_container_width=True, key=f"{data_key}_{current_region}")
        
        elif num_charts == 2:
            # 2 charts side by side
            col1, col2 = st.columns(2)
            
            data_key1, plot_func1 = available_charts[0]
            title1 = data_key1.replace("_", " ").title()
            with col1:
                fig = plot_func1(data[data_key1], title1, unit)
                fig.update_layout(height=820)  # Full screen height
                st.plotly_chart(fig, use_container_width=True, key=f"{data_key1}_{current_region}")
            
            data_key2, plot_func2 = available_charts[1]
            title2 = data_key2.replace("_", " ").title()
            with col2:
                fig = plot_func2(data[data_key2], title2, unit)
                fig.update_layout(height=820)  # Full screen height
                st.plotly_chart(fig, use_container_width=True, key=f"{data_key2}_{current_region}") 
        
        else:
            # Multiple charts - calculate layout
            num_rows = (num_charts + 1) // 2
            
            # Calculate height based on number of rows to fit screen
            # Screen height ~1080px - header(~200px) - legend(~60px) - margins(~40px) = ~780px available
            # Subtract spacing between rows: ~40px per gap
            available_height = 780
            row_spacing = 40 * (num_rows - 1)
            total_chart_height = available_height - row_spacing
            chart_height = int(total_chart_height / num_rows)
            
            # Ensure minimum readable height
            chart_height = max(chart_height, 280)
            
            chart_index = 0
            for row in range(num_rows):
                st.markdown(f'<div class="chart-row" style="animation-delay: {row * 0.1}s;">', unsafe_allow_html=True)
                
                remaining_charts = num_charts - chart_index
                num_cols = min(2, remaining_charts)
                
                if num_cols == 1:

                    data_key, plot_func = available_charts[chart_index]
                    title = data_key.replace("_", " ").title().replace("Fill", "Refill")
                    fig = plot_func(data[data_key], title, unit)
                    fig.update_layout(height=chart_height)
                    st.plotly_chart(fig, use_container_width=True, key=f"{data_key}_{current_region}")
                    chart_index += 1
                else:

                    cols = st.columns(2)
                    
                    for col_idx, col in enumerate(cols):
                        if chart_index < num_charts:
                            data_key, plot_func = available_charts[chart_index]
                            title = data_key.replace("_", " ").title().replace("Fill", "Refill")
                            with col:
                                fig = plot_func(data[data_key], title, unit)
                                fig.update_layout(height=chart_height)
                                st.plotly_chart(fig, use_container_width=True, key=f"{data_key}_{current_region}")
                            chart_index += 1
                
                st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

# EXPORT DATA tab

with tabs[7]:  
    st.markdown(
        "<h2 style='text-align:center;'>📥 Export Dashboard Data</h2>",
        unsafe_allow_html=True
    )
    
    st.markdown("---")
    
    st.info(
        "**Export Instructions:** Select a region and data type below to download the raw data as CSV. "
        "All exports respect the selected date range from the dashboard."
    )
    

    export_region = st.selectbox(
        "Select Region",
        options=["IND", "NASA", "EU", "FML"],
        key="export_region_select"
    )
    
    st.markdown("---")
    

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🚛 DPL Data (On-Highway)")
        

        if not RESULTS[export_region]["theft_raw"].empty:
            theft_csv = RESULTS[export_region]["theft_raw"].to_csv(index=False)
            st.download_button(
                label=f"📥 Download DPL Theft Alerts ({len(RESULTS[export_region]['theft_raw'])} records)",
                data=theft_csv,
                file_name=f"{export_region}_DPL_Theft_{START_DATE}_{END_DATE}.csv",
                mime="text/csv",
                key=f"download_theft_{export_region}"
            )
        else:
            st.info("No DPL theft data available")
        

        if not RESULTS[export_region]["fill_raw"].empty:
            fill_csv = RESULTS[export_region]["fill_raw"].to_csv(index=False)
            st.download_button(
                label=f"📥 Download DPL Filling Alerts ({len(RESULTS[export_region]['fill_raw'])} records)",
                data=fill_csv,
                file_name=f"{export_region}_DPL_Filling_{START_DATE}_{END_DATE}.csv",
                mime="text/csv",
                key=f"download_fill_{export_region}"
            )
        else:
            st.info("No DPL filling data available")
    
    with col2:
        st.subheader("🚜 CEV Data (Off-Highway)")
        

        if not RESULTS[export_region]["theft_cev"].empty:
            theft_cev_csv = RESULTS[export_region]["theft_cev"].to_csv(index=False)
            st.download_button(
                label=f"📥 Download CEV Theft Alerts ({len(RESULTS[export_region]['theft_cev'])} records)",
                data=theft_cev_csv,
                file_name=f"{export_region}_CEV_Theft_{START_DATE}_{END_DATE}.csv",
                mime="text/csv",
                key=f"download_theft_cev_{export_region}"
            )
        else:
            st.info("No CEV theft data available")
        

        if not RESULTS[export_region]["fill_cev"].empty:
            fill_cev_csv = RESULTS[export_region]["fill_cev"].to_csv(index=False)
            st.download_button(
                label=f"📥 Download CEV Filling Alerts ({len(RESULTS[export_region]['fill_cev'])} records)",
                data=fill_cev_csv,
                file_name=f"{export_region}_CEV_Filling_{START_DATE}_{END_DATE}.csv",
                mime="text/csv",
                key=f"download_fill_cev_{export_region}"
            )
        else:
            st.info("No CEV filling data available")
    
    st.markdown("---")
    

    st.subheader("📊 Probable Variation Data")
    
    col3, col4 = st.columns(2)
    
    with col3:

        pv_theft = RESULTS[export_region]["theft_raw"][
            ~RESULTS[export_region]["theft_raw"]["probable_variation_max"].isna()
        ] if "probable_variation_max" in RESULTS[export_region]["theft_raw"].columns else pd.DataFrame()
        
        if not pv_theft.empty:
            pv_theft_csv = pv_theft.to_csv(index=False)
            st.download_button(
                label=f"📥 Download PV Theft ({len(pv_theft)} records)",
                data=pv_theft_csv,
                file_name=f"{export_region}_PV_Theft_{START_DATE}_{END_DATE}.csv",
                mime="text/csv",
                key=f"download_pv_theft_{export_region}"
            )
        else:
            st.info("No probable variation theft data available")
    
    with col4:

        pv_fill = RESULTS[export_region]["fill_raw"][
            ~RESULTS[export_region]["fill_raw"]["probable_variation_max"].isna()
        ] if "probable_variation_max" in RESULTS[export_region]["fill_raw"].columns else pd.DataFrame()
        
        if not pv_fill.empty:
            pv_fill_csv = pv_fill.to_csv(index=False)
            st.download_button(
                label=f"📥 Download PV Filling ({len(pv_fill)} records)",
                data=pv_fill_csv,
                file_name=f"{export_region}_PV_Filling_{START_DATE}_{END_DATE}.csv",
                mime="text/csv",
                key=f"download_pv_fill_{export_region}"
            )
        else:
            st.info("No probable variation filling data available")
    
    st.markdown("---")
    

    st.subheader("🏷️ USFS Tagged Data")
    
    col5, col6 = st.columns(2)
    
    with col5:

        usfs_theft = RESULTS[export_region]["theft_raw"][
            RESULTS[export_region]["theft_raw"]["usfs"].apply(
                lambda x: isinstance(x, list) and any(v in ["usfs", "cusfs"] for v in x) if x else False
            )
        ] if "usfs" in RESULTS[export_region]["theft_raw"].columns and not RESULTS[export_region]["theft_raw"].empty else pd.DataFrame()
        
        if not usfs_theft.empty:
            usfs_theft_csv = usfs_theft.to_csv(index=False)
            st.download_button(
                label=f"📥 Download USFS Theft ({len(usfs_theft)} records)",
                data=usfs_theft_csv,
                file_name=f"{export_region}_USFS_Theft_{START_DATE}_{END_DATE}.csv",
                mime="text/csv",
                key=f"download_usfs_theft_{export_region}"
            )
        else:
            st.info("No USFS theft data available")
    
    with col6:

        usfs_fill = RESULTS[export_region]["fill_raw"][
            RESULTS[export_region]["fill_raw"]["usfs"].apply(
                lambda x: isinstance(x, list) and any(v in ["usfs", "cusfs"] for v in x) if x else False
            )
        ] if "usfs" in RESULTS[export_region]["fill_raw"].columns and not RESULTS[export_region]["fill_raw"].empty else pd.DataFrame()
        
        if not usfs_fill.empty:
            usfs_fill_csv = usfs_fill.to_csv(index=False)
            st.download_button(
                label=f"📥 Download USFS Filling ({len(usfs_fill)} records)",
                data=usfs_fill_csv,
                file_name=f"{export_region}_USFS_Filling_{START_DATE}_{END_DATE}.csv",
                mime="text/csv",
                key=f"download_usfs_fill_{export_region}"
            )
        else:
            st.info("No USFS filling data available")
    
    st.markdown("---")
    

    st.subheader("⚠️ low fuel & Data loss Alert Data")
    
    col7, col8 = st.columns(2)
    
    with col7:

        if not RESULTS[export_region]["low_fuel_raw"].empty:
            low_fuel_csv = RESULTS[export_region]["low_fuel_raw"].to_csv(index=False)
            st.download_button(
                label=f"📥 Download Low Fuel Alerts ({len(RESULTS[export_region]['low_fuel_raw'])} records)",
                data=low_fuel_csv,
                file_name=f"{export_region}_Low_Fuel_Alerts_{START_DATE}_{END_DATE}.csv",
                mime="text/csv",
                key=f"download_low_fuel_{export_region}"
            )
        else:
            st.info("No low fuel alert data available")
    
    with col8:

        if not RESULTS[export_region]["data_loss_raw"].empty:
            data_loss_csv = RESULTS[export_region]["data_loss_raw"].to_csv(index=False)
            st.download_button(
                label=f"📥 Download Data Loss Alerts ({len(RESULTS[export_region]['data_loss_raw'])} records)",
                data=data_loss_csv,
                file_name=f"{export_region}_Data_Loss_{START_DATE}_{END_DATE}.csv",
                mime="text/csv",
                key=f"download_data_loss_{export_region}"
            )
        else:
            st.info("No data loss events available")
    
    st.markdown("---")
    

    st.subheader("📦 Combined Export")
    st.markdown("Download all available data for the selected region in a single CSV file with a 'Data Type' column.")
    
    if st.button(f"📦 Generate Combined Export for {export_region}", key=f"combined_export_{export_region}"):

        all_data = []
        

        if not RESULTS[export_region]["theft_raw"].empty:
            df = RESULTS[export_region]["theft_raw"].copy()
            df["Data_Type"] = "DPL_Theft"
            all_data.append(df)
        

        if not RESULTS[export_region]["fill_raw"].empty:
            df = RESULTS[export_region]["fill_raw"].copy()
            df["Data_Type"] = "DPL_Filling"
            all_data.append(df)
        

        if not RESULTS[export_region]["theft_cev"].empty:
            df = RESULTS[export_region]["theft_cev"].copy()
            df["Data_Type"] = "CEV_Theft"
            all_data.append(df)
        

        if not RESULTS[export_region]["fill_cev"].empty:
            df = RESULTS[export_region]["fill_cev"].copy()
            df["Data_Type"] = "CEV_Filling"
            all_data.append(df)
        

        pv_theft = RESULTS[export_region]["theft_raw"][
            ~RESULTS[export_region]["theft_raw"]["probable_variation_max"].isna()
        ] if "probable_variation_max" in RESULTS[export_region]["theft_raw"].columns else pd.DataFrame()
        
        if not pv_theft.empty:
            df = pv_theft.copy()
            df["Data_Type"] = "PV_Theft"
            all_data.append(df)
        

        pv_fill = RESULTS[export_region]["fill_raw"][
            ~RESULTS[export_region]["fill_raw"]["probable_variation_max"].isna()
        ] if "probable_variation_max" in RESULTS[export_region]["fill_raw"].columns else pd.DataFrame()
        
        if not pv_fill.empty:
            df = pv_fill.copy()
            df["Data_Type"] = "PV_Filling"
            all_data.append(df)
        

        if "usfs" in RESULTS[export_region]["theft_raw"].columns:
            usfs_theft = RESULTS[export_region]["theft_raw"][
                RESULTS[export_region]["theft_raw"]["usfs"].apply(
                    lambda x: isinstance(x, list) and any(v in ["usfs", "cusfs"] for v in x) if x else False
                )
            ]
            if not usfs_theft.empty:
                df = usfs_theft.copy()
                df["Data_Type"] = "USFS_Theft"
                all_data.append(df)
        

        if "usfs" in RESULTS[export_region]["fill_raw"].columns:
            usfs_fill = RESULTS[export_region]["fill_raw"][
                RESULTS[export_region]["fill_raw"]["usfs"].apply(
                    lambda x: isinstance(x, list) and any(v in ["usfs", "cusfs"] for v in x) if x else False
                )
            ]
            if not usfs_fill.empty:
                df = usfs_fill.copy()
                df["Data_Type"] = "USFS_Filling"
                all_data.append(df)
        

        if not RESULTS[export_region]["low_fuel_raw"].empty:
            df = RESULTS[export_region]["low_fuel_raw"].copy()
            df["Data_Type"] = "Low_Fuel_Alert"
            all_data.append(df)
        

        if not RESULTS[export_region]["data_loss_raw"].empty:
            df = RESULTS[export_region]["data_loss_raw"].copy()
            df["Data_Type"] = "Data_Loss"
            all_data.append(df)
        
        if all_data:

            combined_df = pd.concat(all_data, ignore_index=True, sort=False)
            

            cols = combined_df.columns.tolist()
            cols = ['Data_Type'] + [col for col in cols if col != 'Data_Type']
            combined_df = combined_df[cols]
            

            combined_csv = combined_df.to_csv(index=False)
            
            st.download_button(
                label=f"⬇️ Download {export_region} Combined Export (CSV)",
                data=combined_csv,
                file_name=f"{export_region}_Combined_Export_{START_DATE}_{END_DATE}.csv",
                mime="text/csv",
                key=f"combined_download_{export_region}"
            )
            
            st.success(f"Combined export ready for {export_region}! Total records: {len(combined_df):,}")
        else:
            st.warning(f"No data available for {export_region} in the selected date range.")

st.caption("© Intangles | Fuel Monitoring Dashboard")