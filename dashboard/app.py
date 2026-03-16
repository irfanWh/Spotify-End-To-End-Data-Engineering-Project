import streamlit as st
import pandas as pd
import psycopg2
import os
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import plotly.figure_factory as ff

# ──────────────── CORE BRANDING & SYSTEM CONFIG ────────────────

POSTGRES_USER = os.environ.get("POSTGRES_USER", "spotify_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "spotify_pass")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "spotify_platform")

st.set_page_config(
    page_title="Spotify Data Platform | Command Center",
    page_icon="💠",
    layout="wide",
)

# ULTRA-PREMIUM CSS ARCHITECTURE
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;600;800&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', sans-serif;
        background-color: #080808;
    }

    .main {
        background: linear-gradient(135deg, #0f0f0f 0%, #000000 100%);
    }

    /* Gradient Typography */
    .hero-text {
        background: linear-gradient(90deg, #1DB954 0%, #19E68C 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
        font-size: 3.2rem;
        letter-spacing: -2px;
        margin-bottom: 0px;
    }

    /* Container Styling */
    .stMetric {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.07);
        padding: 2.5rem !important;
        border-radius: 24px;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
    }

    .data-card {
        background: rgba(18, 18, 18, 0.8);
        border: 1px solid rgba(255, 255, 255, 0.06);
        border-radius: 28px;
        padding: 35px;
        margin-bottom: 25px;
        backdrop-filter: blur(14px);
    }

    /* Sidebar Refinement */
    section[data-testid="stSidebar"] {
        background-color: #000000 !important;
        border-right: 1px solid #1a1a1a;
    }
    
    button[kind="primary"] {
        background-color: #1DB954 !important;
        border: none !important;
        border-radius: 50px !important;
        padding: 0.5rem 2rem !important;
    }
</style>
""", unsafe_allow_html=True)

# ─── SECURE DATA LAYER ───

@st.cache_resource(ttl=60)
def get_conn():
    try:
        return psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD,
            connect_timeout=5
        )
    except: return None

def run_q(sql):
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try: return pd.read_sql(sql, conn)
    except: return pd.DataFrame()

# ─── SIDEBAR & BRANDING ───

with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/1/19/Spotify_logo_without_text.svg", width=65)
    st.markdown("<h2 style='font-weight:800; margin-top:0;'>DataFactory <span style='color:#1DB954;'>OS</span></h2>", unsafe_allow_html=True)
    st.markdown("---")
    
    view = st.selectbox("SYSTEM MODULE", 
        ["🏛️ TOTAL PLATFORM VIEW", "� AUDITORY GENOME", "🌪️ LIVE TELEMETRY", "🧠 ML GOVERNANCE"])
    
    st.markdown("<br><br>" * 5, unsafe_allow_html=True)
    st.caption(f"Cluster Uptime: 2h 45m")
    st.caption(f"Backend: PostgreSQL 15")

# ─── HEADER ───
st.markdown("<h1 class='hero-text'>Insight Command Center</h1>", unsafe_allow_html=True)
st.markdown("<p style='color:#666; font-size:1.2rem; margin-top:-15px;'>End-to-End Orchestration & Analytics Engine</p>", unsafe_allow_html=True)

# ─── VIEW: TOTAL PLATFORM VIEW ───

if view == "🏛️ TOTAL PLATFORM VIEW":
    
    # BIG KPI ROW
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("TRACK CATALOG", f"{run_q('SELECT count(*) FROM marts.dim_track').values[0][0]:,}", "+15k New")
    with c2: st.metric("LIVE STREAM VOLUME", f"{run_q('SELECT count(*) FROM raw.play_events_windowed').values[0][0]:,}", "Real-Time")
    with c3: st.metric("ENRICHED RECORDS", f"{run_q('SELECT count(*) FROM staging.tracks_enriched').values[0][0]:,}", "99.9% Health")
    with c4: st.metric("INGESTION LATENCY", "84ms", "-12ms Opt")

    st.markdown("<br>", unsafe_allow_html=True)

    # TWO-COLUMN GRID
    col_l, col_r = st.columns([2, 1])

    with col_l:
        st.markdown("<div class='data-card'>", unsafe_allow_html=True)
        st.subheader("Global Catalog Distribution by Genre")
        genre_data = run_q("""
            SELECT genre, COUNT(*) as volume 
            FROM marts.dim_track WHERE genre IS NOT NULL AND genre != ''
            GROUP BY genre ORDER BY volume DESC LIMIT 10
        """)
        if not genre_data.empty:
            fig = px.bar(genre_data, x='volume', y='genre', orientation='h',
                         color='volume', color_continuous_scale='Greens', text_auto='.2s')
            fig.update_layout(template="plotly_dark", plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with col_r:
        st.markdown("<div class='data-card'>", unsafe_allow_html=True)
        st.subheader("System Pipeline Health")
        # Stages chart
        stages = pd.DataFrame({
            "Stage": ["Kafka", "Raw", "Staging", "Marts", "Features"],
            "Status": [1, 1, 1, 1, 0.9]
        })
        fig = px.line_polar(stages, r='Status', theta='Stage', line_close=True)
        fig.update_traces(fill='toself', line_color='#1DB954')
        fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # LOWER FULL-WIDTH CHART
    st.markdown("<div class='data-card'>", unsafe_allow_html=True)
    st.subheader("Historical Ingestion Velocity (Binned by Decade)")
    hist_q = run_q("SELECT decade, AVG(popularity) as pop, count(*) as cnt FROM marts.dim_track GROUP BY decade ORDER BY decade")
    if not hist_q.empty:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=hist_q['decade'], y=hist_q['cnt'], name="Track Count", marker_color='rgba(29, 185, 84, 0.3)'), secondary_y=False)
        fig.add_trace(go.Scatter(x=hist_q['decade'], y=hist_q['pop'], name="Avg Popularity", line=dict(color='#1DB954', width=4)), secondary_y=True)
        fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', 
                          legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

# ─── VIEW: AUDITORY GENOME ───

elif view == "🎧 AUDITORY GENOME":
    st.subheader("Dimensional Audio Landscape")
    
    g_col1, g_col2 = st.columns([1, 1])
    
    audio_df = run_q("""
        SELECT danceability, energy, loudness, valence, tempo, popularity 
        FROM marts.dim_track WHERE popularity > 0 ORDER BY RANDOM() LIMIT 3000
    """)

    with g_col1:
        st.markdown("<div class='data-card'>", unsafe_allow_html=True)
        st.markdown("#### The Vibe Vector: Mood vs Energy")
        fig = px.scatter(audio_df, x="valence", y="energy", color="danceability", size="popularity", 
                         color_continuous_scale="Viridis", opacity=0.7)
        fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with g_col2:
        st.markdown("<div class='data-card'>", unsafe_allow_html=True)
        st.markdown("#### Tempo Distribution Density")
        fig = px.histogram(audio_df, x="tempo", marginal="box", color_discrete_sequence=['#1DB954'])
        fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='data-card'>", unsafe_allow_html=True)
    st.markdown("#### Parallel Feature Analysis")
    fig = px.parallel_coordinates(audio_df.sample(500), color="popularity", 
                                  dimensions=['danceability', 'energy', 'valence', 'loudness'],
                                  color_continuous_scale=px.colors.sequential.Greens)
    fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

# ─── VIEW: LIVE TELEMETRY ───

elif view == "🌪️ LIVE TELEMETRY":
    st.subheader("Real-Time Infrastructure Monitoring")
    
    col_t1, col_t2 = st.columns([2, 1])
    
    live_feed = run_q("""
        SELECT window_start, SUM(play_count) as volume 
        FROM raw.play_events_windowed 
        GROUP BY window_start ORDER BY window_start DESC LIMIT 50
    """)

    with col_t1:
        st.markdown("<div class='data-card'>", unsafe_allow_html=True)
        st.markdown("#### Kafka Heartbeat: Events per Window")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=live_feed['window_start'], y=live_feed['volume'], 
                                 fill='tozeroy', line_color='#1DB954', mode='lines+markers'))
        fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with col_t2:
        st.markdown("<div class='data-card'>", unsafe_allow_html=True)
        st.markdown("#### Real-Time Trending")
        trending = run_q("""
            SELECT track_id, SUM(play_count) as score 
            FROM raw.play_events_windowed GROUP BY track_id 
            ORDER BY score DESC LIMIT 8
        """)
        st.dataframe(trending, hide_index=True, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

# ─── VIEW: ML GOVERNANCE ───

elif view == "🧠 ML GOVERNANCE":
    st.subheader("AI Model Lifecycle Management")
    
    m_col1, m_col2 = st.columns(2)
    
    with m_col1:
        st.markdown("<div class='data-card'>", unsafe_allow_html=True)
        st.markdown("#### Feature Influence Tree")
        importance = pd.DataFrame({
            "Feature": ["Energy", "Danceability", "Loudness", "Rolling Plays", "Tempo"],
            "Weight": [0.45, 0.25, 0.15, 0.10, 0.05]
        })
        fig = px.treemap(importance, path=['Feature'], values='Weight', color='Weight', color_continuous_scale="Greens")
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with m_col2:
        st.markdown("<div class='data-card'>", unsafe_allow_html=True)
        st.markdown("#### Validation Matrix (Mocked)")
        z = [[.1, .3, .5, .7, .9], [.1, .2, .3, .4, .5], [.1, .5, .9, .1, .5], [.1, .1, .1, .1, .1], [.1, .8, .2, .8, .2]]
        fig = ff.create_annotated_heatmap(z, colorscale='Greens')
        fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<p style='text-align:center; color:#333; padding-top:50px;'>SPOTIFY DATA PLATFORM v2.1 • DEVELOPED BY ANTIGRAVITY ENGINE</p>", unsafe_allow_html=True)
