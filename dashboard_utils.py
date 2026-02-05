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


def load_image_base64(image_path):
    try:
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except FileNotFoundError:
        return ""

LOGO_BASE64 = load_image_base64("assets/logo.png")



UNIT_MAP = {
    "IND": "Liters",
    "NASA": "Gallons",
    "EU": "Liters",
    "FML": "Liters"
}


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

end_date = pd.Timestamp.now() - pd.Timedelta(days=2)
start_date = end_date - pd.Timedelta(days=10)
start_time_ms = int(pd.Timestamp(start_date).normalize().timestamp() * 1000)

end_time_ms = int((pd.Timestamp(end_date).normalize() + pd.Timedelta(days=1)).timestamp() * 1000)

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

def create_plot(df, title, unit):
    fig = go.Figure()

    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    fig.add_trace(go.Scatter(
        x=df["time"], y=df["amount"],
        mode="markers+lines", name="Amount",
        line=dict(width=4.5),  # Increased line thickness
        marker=dict(size=9)
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
                width=4.5
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
                f"<b style='font-size:30px'>{title}</b><br><br>"
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
        ,line=dict(width=4.5),  # Increased line thickness
        marker=dict(size=9)
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
                width=4.5
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
                f"<b style='font-size:30px'>{title}</b><br><br>"
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
        line=dict(width=3.5),  # Increased line thickness
        marker=dict(size=9)
    ))

    if "moving average" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["time"],
            y=df["moving average"],
            mode="lines",
            name="Moving Avg",
            line=dict(dash="dot", color="red", width=5.5)
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
                f"<b style='font-size:30px'>{title}</b><br><br>"
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
        line=dict(width=4.5),  # Increased line thickness
        marker=dict(size=9)
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
                width=4.5
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
                f"<b style='font-size:30px'>{title}</b><br><br>"
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

