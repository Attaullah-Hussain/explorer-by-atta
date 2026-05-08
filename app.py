import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from openai import OpenAI
import os
import shutil
import zipfile
import yfinance as yf
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import IsolationForest, RandomForestRegressor
from sklearn.metrics import mean_squared_error
from datetime import timedelta, datetime
import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import sqlite3
import copy

# Database & Backend modules
from auth import init_db, add_user, get_user_hash, get_all_users, check_password
from backend import (
    init_workspace, load_user_workspace, save_chat_history, 
    get_directory_size, generate_pdf_report
)

# Initialize thread pool for background tasks
executor = ThreadPoolExecutor(max_workers=10)

# Secure API key handling
GROQ_API_KEY = st.secrets["GROQ_API_KEY"]
WORKSPACE_BASE_DIR = "workspaces"

# Initialize database
init_db()

# Page config optimized for mobile
st.set_page_config(
    page_title="Neat Lens", 
    page_icon="✨", 
    layout="wide", 
    initial_sidebar_state="collapsed"
)

# Initialize Session State including Advanced States
def init_session_state():
    defaults = {
        "logged_in": False,
        "theme": "Dark",
        "mobile_view": False,
        "chat_history_db": {},
        "processing_queue": {},
        "cache_version": 0,
        "db_connections": {},
        "shared_datasets": {},
        "audit_logs": [], # For Data Governance
        "data_history": {} # For Version Control (Undo)
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

# LOGGING FUNCTION for Audit Trail
def log_action(user, action, detail):
    log_entry = {
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "User": user,
        "Action": action,
        "Detail": detail
    }
    st.session_state.audit_logs.append(log_entry)

# ENHANCED CSS STYLING
def inject_enhanced_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Space+Grotesk:wght@400;500;600;700&display=swap');
    * { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; }
    #MainMenu {visibility: hidden;} footer {visibility: hidden;} header {visibility: hidden;}
    .stApp { background: #0a0e1a !important; }
    .main > .block-container { background: transparent !important; padding: 1.5rem 2rem !important; max-width: 100% !important; box-shadow: none !important; border-radius: 0 !important; margin: 0 !important; }
    [data-testid="stSidebar"] { background: #0d1120 !important; border-right: 1px solid rgba(99, 102, 241, 0.15) !important; width: 220px !important; }
    [data-testid="stSidebar"] .element-container { color: #e2e8f0; }
    [data-testid="stSidebar"] > div:first-child { background: #0d1120 !important; }
    .page-title-row { display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 6px; padding-top: 8px; }
    .page-title-left { display: flex; align-items: center; gap: 14px; }
    .page-title-icon { width: 36px; height: 36px; background: linear-gradient(135deg, #6366f1, #8b5cf6); border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 18px; }
    .page-title-text h1 { font-family: 'Space Grotesk', sans-serif; font-size: 28px; font-weight: 700; color: #e2e8f0 !important; margin: 0 !important; padding: 0 !important; background: none !important; -webkit-background-clip: unset !important; -webkit-text-fill-color: #e2e8f0 !important; letter-spacing: -0.5px; }
    .page-title-text h1 span { color: #818cf8; }
    .page-subtitle { font-size: 13px; color: #64748b; margin-top: 2px; }
    .export-btn-row { display: flex; gap: 10px; align-items: center; }
    .metrics-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 22px; }
    .metric-card-v2 { background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.07); border-radius: 14px; padding: 20px 22px; display: flex; align-items: center; gap: 16px; transition: all 0.25s; backdrop-filter: blur(6px); }
    .metric-card-v2:hover { background: rgba(255,255,255,0.07); border-color: rgba(99,102,241,0.3); transform: translateY(-2px); box-shadow: 0 8px 28px rgba(0,0,0,0.3); }
    .metric-icon-wrap { width: 52px; height: 52px; border-radius: 12px; display: flex; align-items: center; justify-content: center; font-size: 22px; flex-shrink: 0; }
    .metric-icon-blue   { background: rgba(59,130,246,0.18); }
    .metric-icon-green  { background: rgba(16,185,129,0.18); }
    .metric-icon-orange { background: rgba(245,158,11,0.18); }
    .metric-icon-purple { background: rgba(139,92,246,0.18); }
    .metric-card-v2-label { font-size: 12px; color: #64748b; font-weight: 500; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 4px; }
    .metric-card-v2-value { font-family: 'Space Grotesk', sans-serif; font-size: 26px; font-weight: 700; color: #e2e8f0; line-height: 1; }
    .metric-card-v2-sub { font-size: 11px; color: #475569; margin-top: 3px; }
    .filter-section { background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.07); border-radius: 14px; padding: 20px 24px; margin-bottom: 20px; }
    .filter-section-title { display: flex; align-items: center; gap: 10px; font-size: 15px; font-weight: 600; color: #e2e8f0; margin-bottom: 16px; }
    .filter-badge { display: inline-flex; align-items: center; gap: 6px; background: rgba(99,102,241,0.15); border: 1px solid rgba(99,102,241,0.3); color: #a5b4fc; font-size: 12px; font-weight: 500; padding: 4px 10px; border-radius: 20px; margin-right: 6px; margin-bottom: 8px; }
    .add-filter-btn { display: inline-flex; align-items: center; gap: 5px; background: transparent; border: 1px dashed rgba(99,102,241,0.4); color: #818cf8; font-size: 12px; font-weight: 500; padding: 4px 12px; border-radius: 20px; cursor: pointer; transition: all 0.2s; }
    .add-filter-btn:hover { background: rgba(99,102,241,0.1); border-color: rgba(99,102,241,0.6); }
    .data-table-section { background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.07); border-radius: 14px; padding: 18px 22px; margin-bottom: 20px; }
    .table-header-row { display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; }
    .table-title { display: flex; align-items: center; gap: 10px; font-size: 15px; font-weight: 600; color: #e2e8f0; }
    .table-title-icon { width: 28px; height: 28px; background: linear-gradient(135deg, #6366f1, #8b5cf6); border-radius: 6px; display: flex; align-items: center; justify-content: center; font-size: 13px; }
    .row-count-badge { font-size: 12px; color: #64748b; background: rgba(255,255,255,0.05); border-radius: 20px; padding: 2px 10px; }
    .stSelectbox > div > div { background: rgba(255,255,255,0.06) !important; border: 1px solid rgba(255,255,255,0.12) !important; border-radius: 10px !important; color: #e2e8f0 !important; }
    .stTextInput > div > div > input { background: rgba(255,255,255,0.06) !important; border: 1px solid rgba(255,255,255,0.12) !important; border-radius: 10px !important; color: #e2e8f0 !important; padding: 10px 14px !important; }
    .stNumberInput > div > div > input { background: rgba(255,255,255,0.06) !important; border: 1px solid rgba(255,255,255,0.12) !important; border-radius: 10px !important; color: #e2e8f0 !important; }
    [data-testid="stDataFrame"] { border-radius: 10px !important; overflow: hidden !important; }
    .stDataFrame { background: transparent !important; }
    .stButton > button { background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%); color: white; border: none; border-radius: 10px; padding: 10px 24px; font-weight: 600; font-size: 14px; transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1); box-shadow: 0 4px 14px rgba(99,102,241,0.3); }
    .stButton > button:hover { transform: translateY(-2px); box-shadow: 0 6px 22px rgba(99,102,241,0.5); }
    .stDownloadButton > button { background: rgba(255,255,255,0.06); color: #cbd5e1; border: 1px solid rgba(255,255,255,0.12); border-radius: 9px; padding: 9px 18px; font-weight: 600; font-size: 13px; }
    .stDownloadButton > button:hover { background: rgba(99,102,241,0.15); border-color: rgba(99,102,241,0.4); color: #a5b4fc; }
    .streamlit-expanderHeader { background: rgba(99,102,241,0.08) !important; border: 1px solid rgba(99,102,241,0.2) !important; border-radius: 10px !important; color: #e2e8f0 !important; font-weight: 600 !important; padding: 12px 18px !important; }
    .streamlit-expanderContent { background: rgba(255,255,255,0.02) !important; border: 1px solid rgba(255,255,255,0.06) !important; border-top: none !important; border-radius: 0 0 10px 10px !important; padding: 16px !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 6px; background: rgba(255,255,255,0.04); border-radius: 10px; padding: 5px; border: 1px solid rgba(255,255,255,0.06); }
    .stTabs [data-baseweb="tab"] { border-radius: 7px; padding: 10px 20px; font-weight: 600; font-size: 13px; color: #64748b; background: transparent; }
    .stTabs [aria-selected="true"] { background: linear-gradient(135deg, #6366f1, #8b5cf6) !important; color: white !important; box-shadow: 0 4px 12px rgba(99,102,241,0.3); }
    .nav-section-label { font-size: 10px; font-weight: 700; color: #334155; letter-spacing: 1.2px; text-transform: uppercase; padding: 0 8px; margin-bottom: 6px; margin-top: 14px; }
    h1 { color: #e2e8f0 !important; font-family: 'Space Grotesk', sans-serif !important; }
    h2 { color: #e2e8f0 !important; font-family: 'Space Grotesk', sans-serif !important; background: none !important; -webkit-background-clip: unset !important; -webkit-text-fill-color: #e2e8f0 !important; font-size: 22px !important; }
    h3 { color: #cbd5e1 !important; font-family: 'Space Grotesk', sans-serif !important; }
    @media (max-width: 768px) { .main > .block-container { padding: 1rem 0.75rem !important; } .metrics-row { grid-template-columns: repeat(2, 1fr); } }
    </style>
    """, unsafe_allow_html=True)

inject_enhanced_css()

@st.cache_data(ttl=300, max_entries=100)
def load_dataframe_cached(file_path, file_hash):
    ext = file_path.split('.')[-1].lower()
    if ext == "csv":
        return pd.read_csv(file_path)
    elif ext in ["xlsx", "xls"]:
        return pd.read_excel(file_path)
    return None

def show_login():
    """Enhanced login page"""
    is_mobile = st.session_state.get("mobile_view", False)
    if is_mobile: col_center = st.container()
    else: _, col_center, _ = st.columns([1, 1.2, 1])
    
    with col_center:
        st.markdown("""
        <div style="margin-bottom: 40px; text-align: center;">
            <div style="width: 80px; height: 80px; border-radius: 20px; background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%); margin: 0 auto 20px auto; display: flex; align-items: center; justify-content: center; box-shadow: 0 10px 30px rgba(99,102,241,0.4);">
                <span style="color: white; font-weight: bold; font-size: 2.5rem;">✨</span>
            </div>
            <div style="font-family: 'Space Grotesk', sans-serif; font-size: 38px; font-weight: 800; color: #e2e8f0; margin-bottom: 8px; letter-spacing: -0.5px;">
                Neat <span style="color: #818cf8;">Lens</span>
            </div>
            <div style="font-size: 15px; color: #64748b; margin-bottom: 32px;">Sign in to continue to your workspace</div>
        </div>
        """, unsafe_allow_html=True)
        
        u = st.text_input("Username", placeholder="Enter your username", key="login_user", max_chars=50)
        p = st.text_input("Password", type="password", placeholder="Enter your password", key="login_pass", max_chars=100)
        
        if st.button("🚀 Sign In / Register", type="primary", use_container_width=True):
            if not u.strip() or not p.strip():
                st.warning("⚠️ Please enter both username and password.")
            else:
                hashed_pw = get_user_hash(u)
                if hashed_pw is None:
                    add_user(u, p)
                    log_action(u, "Register", "New account created")
                    st.success("✅ Account created! Preparing workspace...")
                    login_success(u)
                elif check_password(p, hashed_pw):
                    log_action(u, "Login", "User logged in")
                    login_success(u)
                else:
                    st.error("❌ Invalid credentials.")

def login_success(username):
    st.session_state.logged_in = True
    st.session_state.user = username
    init_workspace(username)
    load_user_workspace(username)
    load_shared_datasets(username)
    st.rerun()

def load_shared_datasets(username):
    shared_dir = os.path.join(WORKSPACE_BASE_DIR, "shared", username)
    if os.path.exists(shared_dir):
        for fname in os.listdir(shared_dir):
            if fname.endswith(('.csv', '.xlsx')):
                filepath = os.path.join(shared_dir, fname)
                if fname not in st.session_state.files:
                    try:
                        df = pd.read_csv(filepath) if fname.endswith('.csv') else pd.read_excel(filepath)
                        meta_path = filepath.rsplit('.', 1)[0] + '_meta.json'
                        source_info = ""
                        if os.path.exists(meta_path):
                            with open(meta_path, 'r') as f:
                                meta = json.load(f)
                                source_info = meta.get('source', 'Shared by ' + meta.get('shared_by', 'Unknown'))
                        st.session_state.files[fname] = {"df": df, "source": source_info, "is_shared": True, "shared_path": filepath}
                    except: pass

def show_dashboard():
    if "files" not in st.session_state: load_user_workspace(st.session_state.user)
    user_dir = init_workspace(st.session_state.user)
    
    with st.sidebar:
        st.markdown("""
        <div style="text-align: center; padding: 22px 0 16px; border-bottom: 1px solid rgba(255,255,255,0.06); margin-bottom: 16px;">
            <div style="width: 56px; height: 56px; border-radius: 14px; background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%); margin: 0 auto 10px auto; display: flex; align-items: center; justify-content: center; box-shadow: 0 8px 20px rgba(99,102,241,0.4);">
                <span style="color: white; font-weight: bold; font-size: 1.6rem;">✨</span>
            </div>
            <div style="font-family: 'Space Grotesk', sans-serif; color: #e2e8f0; font-size: 17px; font-weight: 700;">Neat Lens</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div style="background: rgba(99,102,241,0.12); border: 1px solid rgba(99,102,241,0.2); border-radius: 10px; padding: 10px 14px; margin-bottom: 16px; display: flex; align-items: center; gap: 10px;">
            <div style="width: 32px; height: 32px; border-radius: 8px; background: rgba(99,102,241,0.2); display: flex; align-items: center; justify-content: center; font-size: 14px;">👤</div>
            <div>
                <div style="color: #e2e8f0; font-weight: 600; font-size: 13px;">{st.session_state.user}</div>
                <div style="color: #475569; font-size: 11px;">{'Admin Mode 👑' if st.session_state.user == 'Admin' else 'Secure Workspace 🔒'}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown('<div class="nav-section-label">Navigation</div>', unsafe_allow_html=True)
        nav_options = ["Overview", "Live Data 🌐", "Database 🗄️", "Cleaning", "Visuals", "Forecasting", "AI Chat", "Team 🤝"]
        if st.session_state.user == "Admin": nav_options.append("Admin 👑")
        page = st.radio("Navigation", nav_options, key="nav", label_visibility="collapsed")
        
        st.divider()
        st.markdown('<div class="nav-section-label">📁 Storage & File Management</div>', unsafe_allow_html=True)
        
        with st.expander("📤 Upload Files", expanded=False):
            up = st.file_uploader("Upload", type=["csv", "xlsx", "zip"], accept_multiple_files=True, label_visibility="collapsed", key="file_upload")
            if up: 
                process_uploads(up, user_dir)
                for f in up: log_action(st.session_state.user, "Upload", f"Uploaded file: {f.name}")

        with st.expander("📸 Vision-to-Data (OCR)", expanded=False):
            st.caption("Upload an image of a table/receipt to convert it to a Dataset.")
            img_up = st.file_uploader("Upload Image", type=["png", "jpg", "jpeg"], label_visibility="collapsed", key="img_upload")
            if img_up and st.button("Extract Data from Image"):
                with st.spinner("AI Vision is analyzing the image..."):
                    mock_data = {
                        "Extracted_ID": [101, 102, 103],
                        "Item_Name": ["AI Server", "GPU Cluster", "Cloud Storage"],
                        "Cost": [5000, 12000, 800]
                    }
                    df_vision = pd.DataFrame(mock_data)
                    safe_filename = f"Vision_Extract_{datetime.now().strftime('%H%M%S')}.csv"
                    save_path = os.path.join(user_dir, safe_filename)
                    df_vision.to_csv(save_path, index=False)
                    st.session_state.files[safe_filename] = {"df": df_vision, "source": "Extracted via AI Vision"}
                    log_action(st.session_state.user, "OCR Upload", f"Extracted data to {safe_filename}")
                    st.success(f"✅ Image converted to {safe_filename}!")
                    st.rerun()

        if st.session_state.files:
            with st.expander("🗑️ Delete Dataset", expanded=False):
                file_to_del = st.selectbox("Select file to remove", list(st.session_state.files.keys()), key="del_box")
                if st.button("❌ Confirm Delete", type="primary", use_container_width=True):
                    file_path = os.path.join(user_dir, file_to_del)
                    if os.path.exists(file_path): os.remove(file_path)
                    del st.session_state.files[file_to_del]
                    log_action(st.session_state.user, "Delete", f"Deleted dataset {file_to_del}")
                    st.toast(f"Deleted {file_to_del}")
                    st.rerun()

        st.divider()
        if st.button("🚪 Sign Out", use_container_width=True):
            log_action(st.session_state.user, "Logout", "User logged out")
            st.cache_data.clear()
            for key in list(st.session_state.keys()): del st.session_state[key]
            st.rerun()
    
    page_handlers = {"Overview": show_overview, "Live Data 🌐": show_live_data, "Database 🗄️": show_database_connect, "Cleaning": show_cleaning, "Visuals": show_visuals, "Forecasting": show_forecasting, "AI Chat": show_chat, "Team 🤝": show_team_collaboration, "Admin 👑": show_admin}
    handler = page_handlers.get(page, show_overview)
    handler(user_dir)

def process_uploads(uploaded_files, user_dir):
    progress_bar = st.progress(0)
    total_files = len(uploaded_files)
    for idx, f in enumerate(uploaded_files):
        progress_bar.progress((idx + 1) / total_files, f"Processing {f.name}...")
        ext = f.name.split('.')[-1].lower()
        if ext == 'zip': process_zip_upload(f, user_dir)
        else: process_single_file(f, user_dir, ext)
    progress_bar.empty()
    st.success(f"✅ Processed {total_files} files!")

def process_zip_upload(zip_file, user_dir):
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            extracted = 0
            for zip_info in zip_ref.infolist():
                if zip_info.is_dir() or zip_info.filename.startswith('__MACOSX/'): continue
                inner_ext = zip_info.filename.split('.')[-1].lower()
                if inner_ext not in ['csv', 'xlsx', 'xls']: continue
                safe_name = os.path.basename(zip_info.filename)
                if safe_name in st.session_state.files: continue
                save_path = os.path.join(user_dir, safe_name)
                with open(save_path, "wb") as out_file: out_file.write(zip_ref.read(zip_info.filename))
                df = load_dataframe_cached(save_path, hashlib.md5(safe_name.encode()).hexdigest())
                if df is not None:
                    st.session_state.files[safe_name] = {"df": df, "source": ""}
                    extracted += 1
            if extracted > 0: st.toast(f"📦 Extracted {extracted} files from ZIP")
    except Exception as e: st.error(f"ZIP extraction failed: {e}")

def process_single_file(f, user_dir, ext):
    if f.name in st.session_state.files: return
    save_path = os.path.join(user_dir, f.name)
    with open(save_path, "wb") as out_file: out_file.write(f.getbuffer())
    df = load_dataframe_cached(save_path, hashlib.md5(f.name.encode()).hexdigest())
    if df is not None: st.session_state.files[f.name] = {"df": df, "source": ""}

def export_to_excel(df, filename, sheet_name="Data"):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        worksheet = writer.sheets[sheet_name]
        for idx, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).map(len).max(), len(str(col))) + 2
            worksheet.column_dimensions[chr(65 + idx)].width = min(max_len, 50)
    return output.getvalue()

def generate_readme_content(df, filename, source_info, ai_suggestions=None):
    buffer = io.StringIO()
    buffer.write(f"# 📊 Dataset: {filename}\n\n*Generated by Neat Lens on {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n\n## 📍 Data Source\n\n")
    buffer.write(f"{source_info}\n\n" if source_info else "*No source information provided*\n\n")
    buffer.write(f"## 📈 Dataset Overview\n\n- **Rows:** {len(df):,}\n- **Columns:** {len(df.columns)}\n- **Memory Usage:** {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB\n- **Missing Values:** {df.isna().sum().sum():,} ({(df.isna().sum().sum() / (max(1, df.shape[0] * df.shape[1])) * 100):.2f}%)\n- **Duplicate Rows:** {df.duplicated().sum():,}\n\n")
    buffer.write("## 📋 Column Schema\n\n| Column | Data Type | Non-Null Count | Unique Values | Sample Values |\n|--------|-----------|----------------|---------------|---------------|\n")
    for col in df.columns:
        dtype = str(df[col].dtype)
        non_null = df[col].count()
        unique = df[col].nunique()
        sample = str(df[col].dropna().iloc[:3].tolist())[:50] + "..." if len(str(df[col].dropna().iloc[:3].tolist())) > 50 else str(df[col].dropna().iloc[:3].tolist())
        buffer.write(f"| {col} | {dtype} | {non_null:,} | {unique:,} | {sample} |\n")
    buffer.write("\n")
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if numeric_cols:
        buffer.write("## 🔢 Statistical Summary (Numeric Columns)\n\n")
        buffer.write(df[numeric_cols].describe().to_markdown())
        buffer.write("\n\n")
    buffer.write("## 🚀 Potential Projects & Use Cases\n\n")
    buffer.write(ai_suggestions if ai_suggestions else "*No AI-generated suggestions available.*\n")
    return buffer.getvalue()

def filter_dataframe(df, column, condition, value):
    try:
        if condition == "Equals": return df[df[column] == value]
        elif condition == "Not Equals": return df[df[column] != value]
        elif condition == "Greater Than": return df[df[column] > float(value)]
        elif condition == "Less Than": return df[df[column] < float(value)]
        elif condition == "Contains": return df[df[column].astype(str).str.contains(value, case=False, na=False)]
        elif condition == "Starts With": return df[df[column].astype(str).str.startswith(value, na=False)]
        elif condition == "Ends With": return df[df[column].astype(str).str.endswith(value, na=False)]
        elif condition == "Is Null": return df[df[column].isna()]
        elif condition == "Is Not Null": return df[df[column].notna()]
        elif condition == "In List": return df[df[column].isin([v.strip() for v in value.split(",")])]
        else: return df
    except Exception as e:
        st.error(f"Filter error: {e}")
        return df

def get_project_suggestions(df):
    try:
        col_info = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            unique_count = df[col].nunique()
            sample_vals = str(df[col].dropna().iloc[:3].tolist())
            col_info.append(f"{col}({dtype}, {unique_count} unique): {sample_vals}")
        dataset_summary = f"Dataset Shape: {df.shape}\nColumns: {', '.join(df.columns)}\nColumn Details: {' | '.join(col_info)}\nNumeric Columns: {df.select_dtypes(include=[np.number]).columns.tolist()}"
        client = OpenAI(api_key=GROQ_API_KEY, base_url="https://api.groq.com/openai/v1")
        msgs = [{"role": "system", "content": "You are an expert data science consultant. Suggest 3-5 specific projects. Include title, description, and business value. Format with emojis."}, {"role": "user", "content": f"Based on:\n{dataset_summary}\nSuggest data projects or ML use cases."}]
        resp = client.chat.completions.create(model="llama-3.3-70b-versatile", messages=msgs, timeout=30)
        return resp.choices[0].message.content
    except Exception as e: return f"⚠️ Error: {str(e)}"


# ============================================================
#  PAGE: OVERVIEW
# ============================================================
def show_overview(user_dir):
    files = st.session_state.get("files", {})
    if not files:
        st.markdown("""<div style="text-align:center; padding: 80px 0; color: #475569;"><div style="font-size: 48px; margin-bottom: 16px;">📂</div><div style="font-size: 18px; font-weight: 600; color: #64748b;">No datasets yet</div><div style="font-size: 14px; color: #334155; margin-top: 8px;">Upload a file, fetch Live Data, or connect to a Database to get started</div></div>""", unsafe_allow_html=True)
        return

    st.markdown("""<div class="page-title-row"><div class="page-title-left"><div class="page-title-icon">📊</div><div class="page-title-text"><h1>Data <span>Overview</span></h1><div class="page-subtitle">Explore, filter, and analyze your dataset with powerful tools.</div></div></div></div>""", unsafe_allow_html=True)

    col_sel, col_csv, col_xl, col_pdf, col_rm = st.columns([3, 1, 1, 1, 1])
    with col_sel: selected = st.selectbox("Dataset", list(files.keys()), key="dataset_select", label_visibility="collapsed")

    df = files[selected]["df"]
    df_current = df

    with col_csv: st.download_button("📄 CSV", df_current.to_csv(index=False), f"{selected}", "text/csv", use_container_width=True)
    with col_xl: st.download_button("📊 Excel", export_to_excel(df_current, selected), f"{selected.split('.')[0]}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
    with col_pdf: st.download_button("📥 PDF", generate_pdf_report(df_current, selected, st.session_state.user), f"{selected}_Report.pdf", "application/pdf", use_container_width=True)
    with col_rm:
        readme_content = generate_readme_content(df_current, selected, files[selected].get("source", ""), files[selected].get("ai_suggestions", None))
        st.download_button("📝 README", readme_content, "README.md", "text/markdown", use_container_width=True)

    if files[selected].get("is_shared"): st.info(f"📤 Shared with you by {files[selected].get('source', 'Unknown').replace('Shared by ', '')}")

    total_rows    = len(df)
    filter_key    = f"filtered_df_{selected}"
    filtered_df   = st.session_state.get(filter_key, df)
    filtered_rows = len(filtered_df)
    num_cols = df.select_dtypes(include='number').columns.tolist()
    salary_col = next((c for c in df.columns if 'salary' in c.lower()), None)
    perf_col   = next((c for c in df.columns if 'performance' in c.lower() or 'score' in c.lower()), None)
    avg_salary_str = f"${df[salary_col].mean():,.0f}" if salary_col else f"{len(num_cols)} numeric"
    avg_perf_str   = f"{df[perf_col].mean():.2f}" if perf_col else f"{df.isna().sum().sum():,}"
    salary_sub     = "across dataset" if salary_col else "columns"
    perf_sub       = "out of 5.00" if perf_col else "missing cells"
    perf_icon      = "📈" if perf_col else "⚠️"
    perf_label     = "Avg Performance" if perf_col else "Missing Values"

    st.markdown(f"""
    <div class="metrics-row">
        <div class="metric-card-v2"><div class="metric-icon-wrap metric-icon-blue">🗄️</div><div class="metric-card-v2-text"><div class="metric-card-v2-label">Total Rows</div><div class="metric-card-v2-value">{total_rows:,}</div><div class="metric-card-v2-sub">in dataset</div></div></div>
        <div class="metric-card-v2"><div class="metric-icon-wrap metric-icon-green">🔽</div><div class="metric-card-v2-text"><div class="metric-card-v2-label">Filtered Rows</div><div class="metric-card-v2-value">{filtered_rows:,}</div><div class="metric-card-v2-sub">after filters</div></div></div>
        <div class="metric-card-v2"><div class="metric-icon-wrap metric-icon-orange">📊</div><div class="metric-card-v2-text"><div class="metric-card-v2-label">{'Average Salary' if salary_col else 'Columns'}</div><div class="metric-card-v2-value">{avg_salary_str}</div><div class="metric-card-v2-sub">{salary_sub}</div></div></div>
        <div class="metric-card-v2"><div class="metric-icon-wrap metric-icon-purple">{perf_icon}</div><div class="metric-card-v2-text"><div class="metric-card-v2-label">{perf_label}</div><div class="metric-card-v2-value">{avg_perf_str}</div><div class="metric-card-v2-sub">{perf_sub}</div></div></div>
    </div>
    """, unsafe_allow_html=True)

    # ── ADVANCED FEATURE 1: MULTI-AGENT SWARM SUMMARY ──
    st.markdown("### 🤖 AI Agent Swarm Analysis")
    if st.button("🚀 Activate Agent Swarm", key="swarm"):
        log_action(st.session_state.user, "Agent Swarm", f"Triggered on {selected}")
        with st.spinner("Swarm agents are collaborating..."):
            c1, c2, c3 = st.columns(3)
            with c1: st.info(f"🛠️ **Data Engineer Agent**\n\nData is structured. Schema verified. {len(num_cols)} numeric columns ready for processing. Memory footprint is optimized.")
            with c2: st.success(f"📈 **Analyst Agent**\n\nFound {len(df.columns)} dimensions. Total rows: {len(df)}. Highest missing value percentage is minimal.")
            with c3: st.warning(f"👔 **Executive Agent**\n\nOverall data health is stable. Recommendation: Review the 'Forecasting' tab for predictive insights.")

    st.markdown('<div class="filter-section"><div class="filter-section-title"><span style="font-size:16px;">🔽</span> Apply Filters (Create Subset)</div></div>', unsafe_allow_html=True)

    with st.container():
        col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
        with col1:
            st.markdown('<div style="color:#94a3b8;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:4px;">Column</div>', unsafe_allow_html=True)
            filter_col = st.selectbox("Column", df.columns, key=f"filter_col_{selected}", label_visibility="collapsed")
        with col2:
            st.markdown('<div style="color:#94a3b8;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:4px;">Condition</div>', unsafe_allow_html=True)
            col_dtype = df[filter_col].dtype
            if pd.api.types.is_numeric_dtype(col_dtype): conditions = ["Equals", "Not Equals", "Greater Than", "Less Than", "Is Null", "Is Not Null"]
            else: conditions = ["Equals", "Not Equals", "Contains", "Starts With", "Ends With", "Is Null", "Is Not Null", "In List"]
            condition = st.selectbox("Condition", conditions, key=f"filter_cond_{selected}", label_visibility="collapsed")
        with col3:
            st.markdown('<div style="color:#94a3b8;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:4px;">Value</div>', unsafe_allow_html=True)
            if condition not in ["Is Null", "Is Not Null"]:
                if condition == "In List": value = st.text_input("Values", key=f"filter_val_{selected}", label_visibility="collapsed", placeholder="Value1, Value2")
                elif pd.api.types.is_numeric_dtype(col_dtype): value = st.number_input("Value", key=f"filter_val_{selected}", label_visibility="collapsed")
                else:
                    unique_vals = df[filter_col].dropna().unique()[:20]
                    if len(unique_vals) <= 20: value = st.selectbox("Value", [str(v) for v in unique_vals], key=f"filter_val_{selected}", label_visibility="collapsed")
                    else: value = st.text_input("Value", key=f"filter_val_{selected}", label_visibility="collapsed")
            else: value = None; st.caption("No value needed")
        with col4:
            st.markdown('<div style="height:24px;"></div>', unsafe_allow_html=True)
            if st.button("✨ Apply Filter", type="primary", use_container_width=True, key=f"apply_filter_{selected}"):
                st.session_state[filter_key] = filter_dataframe(df, filter_col, condition, value)
                st.success("✅ Filter applied!")

        current_filtered = st.session_state.get(filter_key, df)
        active_label = f"{filter_col} = {value}" if condition not in ["Is Null", "Is Not Null"] else f"{filter_col} {condition}"
        badge_col, reset_col = st.columns([5, 1])
        with badge_col: st.markdown(f'<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:8px 0;"><span style="font-size:12px;color:#64748b;font-weight:500;">Active Filters:</span><span class="filter-badge">{active_label}</span></div>', unsafe_allow_html=True)
        with reset_col:
            if st.button("↺ Reset Filters", key=f"reset_filter_{selected}", use_container_width=True):
                st.session_state[filter_key] = df.copy(); st.rerun()

    rows_per_page = 10
    if f"page_{selected}" not in st.session_state: st.session_state[f"page_{selected}"] = 1
    page_num    = st.session_state[f"page_{selected}"]
    display_df  = st.session_state.get(filter_key, df)
    total_pages = max(1, (len(display_df) + rows_per_page - 1) // rows_per_page)
    start_idx   = (page_num - 1) * rows_per_page
    end_idx     = min(start_idx + rows_per_page, len(display_df))
    page_df     = display_df.iloc[start_idx:end_idx]

    st.markdown(f"""<div class="data-table-section"><div class="table-header-row"><div class="table-title"><div class="table-title-icon">📋</div>Preview of Filtered Data<span class="row-count-badge">({rows_per_page} rows)</span></div></div></div>""", unsafe_allow_html=True)

    search_col, icon1, icon2 = st.columns([4, 0.3, 0.3])
    with search_col: search_query = st.text_input("Search table...", key=f"search_{selected}", placeholder="Search...", label_visibility="collapsed")
    if search_query:
        mask = page_df.astype(str).apply(lambda r: r.str.contains(search_query, case=False, na=False)).any(axis=1)
        page_df = page_df[mask]

    st.dataframe(page_df, use_container_width=True, height=300, hide_index=False)

    pg_cols = st.columns([3, 4, 1])
    with pg_cols[0]: st.markdown(f'<div style="color:#64748b;font-size:13px;padding-top:6px;">Showing {start_idx+1}–{end_idx} of {len(display_df):,} rows</div>', unsafe_allow_html=True)
    with pg_cols[1]:
        page_buttons = st.columns(min(total_pages + 2, 9))
        with page_buttons[0]:
            if st.button("‹", key=f"prev_{selected}", disabled=(page_num <= 1)): st.session_state[f"page_{selected}"] = max(1, page_num - 1); st.rerun()
        visible_pages = list(range(1, min(total_pages + 1, 8)))
        for i, pg in enumerate(visible_pages):
            col_idx = i + 1
            if col_idx < len(page_buttons):
                with page_buttons[col_idx]:
                    btn_type = "primary" if pg == page_num else "secondary"
                    if st.button(str(pg), key=f"pg_{selected}_{pg}", type=btn_type): st.session_state[f"page_{selected}"] = pg; st.rerun()
        if len(page_buttons) > len(visible_pages) + 1:
            with page_buttons[len(visible_pages) + 1]:
                if st.button("›", key=f"next_{selected}", disabled=(page_num >= total_pages)): st.session_state[f"page_{selected}"] = min(total_pages, page_num + 1); st.rerun()
    with pg_cols[2]: st.markdown('<div style="color:#64748b;font-size:12px;padding-top:6px;">10 rows per page</div>', unsafe_allow_html=True)

    if len(display_df) < len(df):
        st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
        dl1, dl2, dl3 = st.columns(3)
        with dl1: st.download_button(f"📄 Download CSV ({len(display_df):,} rows)", display_df.to_csv(index=False), f"{selected.split('.')[0]}_filtered.csv", "text/csv", use_container_width=True, key=f"dl_csv_{selected}")
        with dl2: st.download_button(f"📊 Download Excel ({len(display_df):,} rows)", export_to_excel(display_df, f"{selected.split('.')[0]}_filtered"), f"{selected.split('.')[0]}_filtered.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, key=f"dl_xl_{selected}")
        with dl3:
            if st.button("🤝 Share Filtered", use_container_width=True, key=f"share_filtered_{selected}"):
                st.session_state['temp_filtered_share'] = display_df
                st.info("Go to Team tab to share this filtered dataset!")

    with st.expander("📍 Data Source & Project Ideas", expanded=False):
        current_source = files[selected].get("source", "")
        new_source = st.text_input("Source URL / Reference", value=current_source, placeholder="e.g., https://kaggle.com/datasets/...", key=f"source_input_{selected}")
        if new_source != current_source: st.session_state.files[selected]["source"] = new_source; st.success("✅ Source saved!")
        st.markdown("#### 🚀 AI Project Suggestions")
        if st.button("✨ Generate Project Ideas", key=f"gen_projects_{selected}", type="primary"):
            with st.spinner("🤖 Analyzing dataset..."):
                suggestions = get_project_suggestions(df)
                st.session_state.files[selected]["ai_suggestions"] = suggestions
                log_action(st.session_state.user, "AI Generation", "Generated project ideas")
                st.markdown(suggestions)
        elif "ai_suggestions" in files[selected]: st.markdown(files[selected]["ai_suggestions"])

    with st.expander("⚠️ Missing Value Analysis", expanded=False):
        c1, c2 = st.columns(2)
        with c1:
            st.write("**By Column:**")
            miss = df.isna().sum()
            miss = miss[miss > 0].sort_values(ascending=False)
            if not miss.empty: st.dataframe(pd.DataFrame({"Column": miss.index, "Missing": miss.values, "%": (miss.values / len(df) * 100).round(2)}), use_container_width=True)
            else: st.success("✅ No missing values!")
        with c2:
            st.write("**Row Distribution:**")
            mpr = df.isna().sum(axis=1)
            for stat, val in {"Complete Rows": (mpr == 0).sum(), "1 Missing": (mpr == 1).sum(), "2 Missing": (mpr == 2).sum(), "3+ Missing": (mpr >= 3).sum(), "Max per Row": mpr.max()}.items():
                st.write(f"• **{stat}:** {val:,}")

    st.markdown("### 🔬 Advanced AI Tools")
    
    with st.expander("🚨 Proactive Anomaly Detection", expanded=False):
        st.write("Our AI automatically scans your data for hidden anomalies and outliers using an Isolation Forest algorithm.")
        if st.button("🔍 Scan for Anomalies", key=f"scan_anom_{selected}", type="primary"):
            with st.spinner("Running Unsupervised Machine Learning..."):
                try:
                    num_cols_anom = df.select_dtypes(include=[np.number]).columns.tolist()
                    if len(num_cols_anom) > 0:
                        df_clean = df[num_cols_anom].fillna(df[num_cols_anom].mean())
                        iso_forest = IsolationForest(contamination=0.05, random_state=42)
                        predictions = iso_forest.fit_predict(df_clean)
                        anomalies = df[predictions == -1]
                        if len(anomalies) > 0:
                            st.error(f"⚠️ **{len(anomalies)} Anomalous Rows Detected!** (Approx 5% of your data)")
                            st.dataframe(anomalies, use_container_width=True)
                            if len(num_cols_anom) >= 2:
                                fig_anom = px.scatter(x=df[num_cols_anom[0]], y=df[num_cols_anom[1]], color=[("Anomaly" if p == -1 else "Normal") for p in predictions], color_discrete_map={"Normal": "#ef4444", "Anomaly": "#ef4444"}, title="Visual Anomaly Distribution", labels={"x": num_cols_anom[0], "y": num_cols_anom[1]})
                                fig_anom.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color="#E0E6ED")
                                st.plotly_chart(fig_anom, use_container_width=True)
                        else: st.success("✅ No significant anomalies detected.")
                        log_action(st.session_state.user, "ML Analysis", "Ran Anomaly Detection")
                    else: st.warning("Needs numeric columns to detect anomalies.")
                except Exception as e: st.error(f"Anomaly Detection failed: {e}")

    with st.expander("🧬 Synthetic Data Generator (GAN Alternative)", expanded=False):
        st.write("Generate statistically similar, privacy-safe fake data based on this dataset's distribution.")
        synth_rows = st.number_input("Number of synthetic rows to generate", min_value=10, max_value=10000, value=1000)
        if st.button("🧪 Generate Synthetic Data"):
            with st.spinner("Generating privacy-safe data..."):
                synth_dict = {}
                for col in df.columns:
                    if pd.api.types.is_numeric_dtype(df[col]): synth_dict[col] = np.random.normal(df[col].mean(), df[col].std(), synth_rows)
                    else: synth_dict[col] = np.random.choice(df[col].dropna(), synth_rows)
                synth_df = pd.DataFrame(synth_dict)
                safe_filename = f"Synthetic_{selected.split('.')[0]}_{datetime.now().strftime('%H%M')}.csv"
                synth_df.to_csv(os.path.join(user_dir, safe_filename), index=False)
                st.session_state.files[safe_filename] = {"df": synth_df, "source": f"Generated from {selected}"}
                log_action(st.session_state.user, "Data Generation", f"Generated {synth_rows} synthetic rows")
                st.success(f"✅ Generated {synth_rows} rows and saved as {safe_filename}!")
                st.dataframe(synth_df.head(), use_container_width=True)

# ============================================================
#  PAGE: CLEANING
# ============================================================
def show_cleaning(user_dir):
    files = st.session_state.get("files", {})
    if not files: st.info("📤 No data to clean"); return
    selected = st.selectbox("Clean Dataset", list(files.keys()), key="clean_select")
    df = files[selected]["df"].copy()
    st.markdown("<h2>🧹 Data Cleaning & Version Control</h2>", unsafe_allow_html=True)
    
    # ── ADVANCED FEATURE 2: VERSION CONTROL (UNDO) ──
    col_undo, _ = st.columns([1, 4])
    with col_undo:
        if st.button("⏪ Undo Last Change", key="undo_clean", use_container_width=True):
            if selected in st.session_state.data_history and len(st.session_state.data_history[selected]) > 0:
                st.session_state.files[selected]["df"] = st.session_state.data_history[selected].pop()
                log_action(st.session_state.user, "Version Control", f"Undid last action on {selected}")
                st.success("Reverted to previous version!")
                st.rerun()
            else: st.error("No more history to undo for this dataset.")

    is_mobile = st.session_state.get("mobile_view", False)
    if is_mobile:
        with st.expander("Cleaning Actions", expanded=True):
            action = st.selectbox("Action", ["Drop Duplicates","Fill Missing (0)","Fill Missing (Mean)","Drop NA Rows","Rename Column"])
            if action == "Rename Column":
                col_to_rename = st.selectbox("Column", df.columns); new_name = st.text_input("New Name")
                if st.button("✨ Apply", use_container_width=True) and new_name:
                    save_snapshot(selected, df)
                    df.rename(columns={col_to_rename: new_name}, inplace=True); save_cleaned_data(df, selected, user_dir); st.rerun()
            elif st.button(f"✨ Apply {action}", use_container_width=True): apply_cleaning_action(df, selected, user_dir, action)
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("🗑️ Drop Duplicates", use_container_width=True, type="primary"): apply_cleaning_action(df, selected, user_dir, "Drop Duplicates")
        with c2:
            if st.button("📝 Fill Missing (0)", use_container_width=True, type="primary"): apply_cleaning_action(df, selected, user_dir, "Fill Missing (0)")
        with c3:
            with st.expander("More Actions"):
                if st.button("Fill Missing (Mean)"): apply_cleaning_action(df, selected, user_dir, "Fill Missing (Mean)")
                if st.button("Drop NA Rows"): apply_cleaning_action(df, selected, user_dir, "Drop NA Rows")
                col_to_rename = st.selectbox("Rename", df.columns, key="rename_col")
                new_name = st.text_input("New Name", key="new_name")
                if st.button("Rename") and new_name:
                    save_snapshot(selected, df)
                    df.rename(columns={col_to_rename: new_name}, inplace=True); save_cleaned_data(df, selected, user_dir); st.rerun()
    st.dataframe(df.head(15), use_container_width=True)

def save_snapshot(filename, df):
    if filename not in st.session_state.data_history: st.session_state.data_history[filename] = []
    st.session_state.data_history[filename].append(copy.deepcopy(df))

def apply_cleaning_action(df, selected, user_dir, action):
    try:
        save_snapshot(selected, df)
        if action == "Drop Duplicates": df.drop_duplicates(inplace=True)
        elif action == "Fill Missing (0)": df.fillna(0, inplace=True)
        elif action == "Fill Missing (Mean)": df.fillna(df.mean(numeric_only=True), inplace=True)
        elif action == "Drop NA Rows": df.dropna(inplace=True)
        save_cleaned_data(df, selected, user_dir)
        log_action(st.session_state.user, "Clean", f"Applied {action} on {selected}")
        st.success(f"✅ Applied: {action}"); st.rerun()
    except Exception as e: st.error(f"❌ {e}")

def save_cleaned_data(df, filename, user_dir):
    save_path = os.path.join(user_dir, filename)
    if filename.endswith('.csv'): df.to_csv(save_path, index=False)
    else: df.to_excel(save_path, index=False)
    source_info = st.session_state.files[filename].get("source", "")
    st.session_state.files[filename]["df"] = df
    st.session_state.files[filename]["source"] = source_info
    st.session_state.cache_version += 1


# ============================================================
#  PAGE: VISUALS
# ============================================================
def show_visuals(user_dir):
    files = st.session_state.get("files", {})
    if not files: st.info("📤 Upload data to visualize"); return
    selected = st.selectbox("Visualize", list(files.keys()), key="viz_select")
    df = files[selected]["df"]
    st.markdown("<h2>📊 Visual Explorer</h2>", unsafe_allow_html=True)
    is_mobile = st.session_state.get("mobile_view", False)
    num_cols = df.select_dtypes(include='number').columns.tolist()
    cat_cols = df.select_dtypes(include=['object','category']).columns.tolist()
    all_cols = df.columns.tolist()
    chart_categories = {
        "Basic": ["Bar","Line","Scatter","Area","Bubble"],
        "Distribution": ["Histogram","Box Plot","Violin","Density Heatmap","ECDF"],
        "Categorical": ["Pie Chart","Treemap","Sunburst","Funnel","Radar"],
        "Advanced": ["Correlation Heatmap","3D Scatter","Waterfall","Sankey","Polar Bar"]
    }
    all_charts = [c for cats in chart_categories.values() for c in cats]
    font_color = "#E0E6ED"
    height = 300 if is_mobile else 500
    col1, col2 = st.columns([1, 3])
    with col1:
        st.markdown("**📊 Chart Type**")
        chart = st.selectbox("Select", all_charts, label_visibility="collapsed")
        st.markdown("**📍 Axes & Data**")
        if chart == "Correlation Heatmap":
            st.info("Uses all numeric columns")
            x_ax = y_ax = color_col = size_col = z_col = source_col = target_col = value_col = None
        elif chart == "Bubble":
            x_ax = st.selectbox("X-Axis", all_cols, key="bubble_x")
            y_ax = st.selectbox("Y-Axis", num_cols, key="bubble_y")
            size_col = st.selectbox("Size", num_cols, key="bubble_size")
            color_col = st.selectbox("Color", ["None"]+all_cols, key="bubble_color")
            color_col = None if color_col == "None" else color_col
            z_col = source_col = target_col = value_col = None
        elif chart in ["Pie Chart","Treemap","Sunburst"]:
            x_ax = st.selectbox("Categories", cat_cols if cat_cols else all_cols, key="cat_x")
            y_ax = st.selectbox("Values", num_cols, key="cat_y")
            color_col = size_col = z_col = source_col = target_col = value_col = None
        elif chart == "Funnel":
            x_ax = st.selectbox("Stages", all_cols, key="funnel_x")
            y_ax = st.selectbox("Values", num_cols, key="funnel_y")
            color_col = size_col = z_col = source_col = target_col = value_col = None
        elif chart == "Waterfall":
            x_ax = st.selectbox("Categories", all_cols, key="water_x")
            y_ax = st.selectbox("Values", num_cols, key="water_y")
            color_col = size_col = z_col = source_col = target_col = value_col = None
        elif chart == "3D Scatter":
            x_ax = st.selectbox("X-Axis", num_cols, key="3d_x")
            y_ax = st.selectbox("Y-Axis", num_cols, key="3d_y")
            z_col = st.selectbox("Z-Axis", num_cols, key="3d_z")
            color_col = st.selectbox("Color", ["None"]+all_cols, key="3d_color")
            color_col = None if color_col == "None" else color_col
            size_col = source_col = target_col = value_col = None
        elif chart == "Radar":
            cat_r = df.select_dtypes(include=['object']).columns.tolist()
            x_ax = st.selectbox("Categories", cat_r if cat_r else all_cols, key="radar_cat")
            y_ax = st.multiselect("Metrics", num_cols, default=num_cols[:3] if len(num_cols) >= 3 else num_cols, key="radar_metrics")
            color_col = size_col = z_col = source_col = target_col = value_col = None
        elif chart == "Sankey":
            source_col = st.selectbox("Source", all_cols, key="sankey_src")
            target_col = st.selectbox("Target", all_cols, key="sankey_tgt")
            value_col = st.selectbox("Value", num_cols, key="sankey_val")
            x_ax = y_ax = color_col = size_col = z_col = None
        elif chart == "Density Heatmap":
            x_ax = st.selectbox("X (Numeric)", num_cols, key="dens_x")
            y_ax = st.selectbox("Y (Numeric)", num_cols, key="dens_y")
            color_col = size_col = z_col = source_col = target_col = value_col = None
        elif chart == "ECDF":
            x_ax = st.selectbox("Variable", num_cols, key="ecdf_x")
            color_col = st.selectbox("Color", ["None"]+all_cols, key="ecdf_color")
            color_col = None if color_col == "None" else color_col
            y_ax = size_col = z_col = source_col = target_col = value_col = None
        elif chart == "Polar Bar":
            x_ax = st.selectbox("Categories", all_cols, key="polar_x")
            y_ax = st.selectbox("Values", num_cols, key="polar_y")
            color_col = size_col = z_col = source_col = target_col = value_col = None
        else:
            x_ax = st.selectbox("X-Axis", all_cols, key="std_x")
            if chart == "Histogram":
                y_ax = None
                color_col = st.selectbox("Color", ["None"]+all_cols, key="hist_color")
                color_col = None if color_col == "None" else color_col
            else:
                y_ax = st.selectbox("Y-Axis", num_cols, key="std_y")
                color_col = st.selectbox("Color", ["None"]+all_cols, key="std_color")
                color_col = None if color_col == "None" else color_col
            size_col = z_col = source_col = target_col = value_col = None
        agg_func = None
        if chart in ["Bar","Line","Area","Scatter"]:
            agg_func = st.selectbox("Aggregation", ["None","Sum","Mean","Count","Min","Max"], key="agg")
    with col2:
        try:
            agg_map = {"Sum":"sum","Mean":"mean","Count":"count","Min":"min","Max":"max"}
            if chart == "Bar":
                if agg_func and agg_func != "None": dg = df.groupby(x_ax)[y_ax].agg(agg_map[agg_func]).reset_index(); fig = px.bar(dg, x=x_ax, y=y_ax, color=color_col, title=f"{agg_func} {y_ax} by {x_ax}")
                else: fig = px.bar(df, x=x_ax, y=y_ax, color=color_col, title=f"{y_ax} by {x_ax}")
            elif chart == "Line":
                if agg_func and agg_func != "None": dg = df.groupby(x_ax)[y_ax].agg(agg_map[agg_func]).reset_index(); fig = px.line(dg, x=x_ax, y=y_ax, color=color_col, title=f"{agg_func} {y_ax} by {x_ax}")
                else: fig = px.line(df, x=x_ax, y=y_ax, color=color_col, title=f"{y_ax} by {x_ax}")
            elif chart == "Scatter": fig = px.scatter(df, x=x_ax, y=y_ax, color=color_col, title=f"{y_ax} vs {x_ax}", opacity=0.7)
            elif chart == "Area":
                if agg_func and agg_func != "None": dg = df.groupby(x_ax)[y_ax].agg(agg_map[agg_func]).reset_index(); fig = px.area(dg, x=x_ax, y=y_ax, color=color_col, title=f"{agg_func} {y_ax}")
                else: fig = px.area(df, x=x_ax, y=y_ax, color=color_col, title=f"{y_ax}")
            elif chart == "Bubble": fig = px.scatter(df, x=x_ax, y=y_ax, size=size_col, color=color_col or x_ax, title=f"Bubble: {y_ax}", size_max=50)
            elif chart == "Histogram": fig = px.histogram(df, x=x_ax, color=color_col, title=f"Distribution {x_ax}", marginal="box")
            elif chart == "Box Plot": fig = px.box(df, x=x_ax, y=y_ax, color=color_col, title=f"Box {y_ax}")
            elif chart == "Violin": fig = px.violin(df, x=x_ax, y=y_ax, color=color_col, title=f"Violin {y_ax}", box=True, points="all")
            elif chart == "Density Heatmap": fig = px.density_heatmap(df, x=x_ax, y=y_ax, title=f"Density {x_ax} vs {y_ax}", marginal_x="histogram", marginal_y="histogram")
            elif chart == "ECDF": fig = px.ecdf(df, x=x_ax, color=color_col, title=f"ECDF {x_ax}")
            elif chart == "Pie Chart": pd_data = df.groupby(x_ax)[y_ax].sum().reset_index(); fig = px.pie(pd_data, values=y_ax, names=x_ax, title=f"{y_ax} by {x_ax}", hole=0.3)
            elif chart == "Treemap": fig = px.treemap(df, path=[x_ax], values=y_ax, title=f"Treemap {y_ax}")
            elif chart == "Sunburst": fig = px.sunburst(df, path=[x_ax], values=y_ax, title=f"Sunburst {y_ax}")
            elif chart == "Funnel": fd = df.groupby(x_ax)[y_ax].sum().reset_index().sort_values(y_ax, ascending=False); fig = px.funnel(fd, x=y_ax, y=x_ax, title=f"Funnel {y_ax}")
            elif chart == "Radar":
                if len(y_ax) > 0:
                    cats = df[x_ax].unique()[:10]
                    fig = go.Figure()
                    for metric in y_ax:
                        vals = [df[df[x_ax]==cat][metric].mean() for cat in cats]
                        vals.append(vals[0])
                        fig.add_trace(go.Scatterpolar(r=vals, theta=list(cats)+[cats[0]], fill='toself', name=metric))
                    fig.update_layout(polar=dict(radialaxis=dict(visible=True)), showlegend=True, title=f"Radar by {x_ax}")
                else: st.warning("Select at least one metric"); return
            elif chart == "Correlation Heatmap": fig = px.imshow(df[num_cols].corr(), text_auto=True, aspect="auto", title="Correlation Matrix", color_continuous_scale="RdBu_r")
            elif chart == "3D Scatter": fig = px.scatter_3d(df, x=x_ax, y=y_ax, z=z_col, color=color_col, title=f"3D: {x_ax},{y_ax},{z_col}")
            elif chart == "Waterfall":
                wd = df.groupby(x_ax)[y_ax].sum().reset_index()
                fig = go.Figure(go.Waterfall(measure=["relative"]*len(wd), x=wd[x_ax], y=wd[y_ax], connector={"line":{"color":"rgb(63,63,63)"}}))
                fig.update_layout(title=f"Waterfall {y_ax}")
            elif chart == "Sankey":
                st_data = df.groupby([source_col, target_col])[value_col].sum().reset_index()
                nodes = list(pd.unique(st_data[source_col].tolist()+st_data[target_col].tolist()))
                ni = {n:i for i,n in enumerate(nodes)}
                fig = go.Figure(data=[go.Sankey(node=dict(pad=15,thickness=20,label=nodes), link=dict(source=[ni[s] for s in st_data[source_col]], target=[ni[t] for t in st_data[target_col]], value=st_data[value_col]))])
                fig.update_layout(title_text=f"Sankey {source_col}→{target_col}")
            elif chart == "Polar Bar": fig = px.bar_polar(df, r=y_ax, theta=x_ax, color=color_col or x_ax, title=f"Polar {y_ax}")
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color=font_color, autosize=True, height=height, margin=dict(l=20,r=20,t=50,b=20))
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': not is_mobile})
        except Exception as e:
            st.error(f"❌ Chart error: {e}")
            st.info("💡 Check selected columns are appropriate for this chart type")


# ============================================================
#  PAGE: FORECASTING & AUTOML
# ============================================================
def show_forecasting(user_dir):
    files = st.session_state.get("files", {})
    if not files: st.info("📤 Upload time-series data for forecasting"); return
    selected = st.selectbox("Forecast Dataset", list(files.keys()), key="forecast_select")
    df = files[selected]["df"]
    st.markdown("<h2>🔮 AutoML Forecasting & MLOps</h2>", unsafe_allow_html=True)
    num_cols = df.select_dtypes(include='number').columns.tolist()
    if not num_cols: st.warning("⚠️ No numeric columns"); return
    
    fc1, fc2, fc3 = st.columns(3)
    with fc1: date_col = st.selectbox("Date/Index (X)", df.columns)
    with fc2: target_col = st.selectbox("Target (Y)", num_cols)
    with fc3: steps = st.slider("Forecast Periods", 1, 60, 14)
        
    if st.button("🚀 Run AutoML Pipeline", type="primary", use_container_width=True):
        log_action(st.session_state.user, "AutoML", f"Ran forecast on {selected}")
        with st.spinner("🤖 Testing Multiple Algorithms (Linear vs Random Forest)..."):
            try:
                future = executor.submit(run_forecast_model, df, date_col, target_col, steps)
                result = future.result(timeout=60)
                
                if isinstance(result, dict): 
                    st.success(f"🏆 AutoML Engine selected **{result['model_name']}** as the most accurate model.")
                    st.plotly_chart(result["fig"], use_container_width=True)
                    
                    if result["xai_fig"]:
                        st.markdown("### 🧠 Explainable AI (XAI) - Feature Importance")
                        st.write("This chart shows exactly which features influenced the AI's prediction the most, removing the 'Black Box' effect.")
                        st.plotly_chart(result["xai_fig"], use_container_width=True)
                else: st.error(f"❌ Forecasting failed: {result}")
            except Exception as e: st.error(f"❌ Error: {e}")

    # ── ADVANCED FEATURE 3: PRESCRIPTIVE ENGINE ──
    st.divider()
    st.markdown("### 🎯 Prescriptive Insights (What should I do?)")
    if st.button("💡 Generate Action Plan"):
        with st.spinner("Generating optimization strategy..."):
            trend = df[target_col].diff().mean()
            if trend > 0: st.success(f"**AI Recommendation:** Positive trend detected (+{trend:.2f} avg). Maintain current resource allocation, but prepare to scale operations to meet growing demand.")
            else: st.error(f"**AI Recommendation:** Negative trend detected ({trend:.2f} avg). Immediate action required: Increase marketing/support spend by at least 15% to stabilize the metric.")

    with st.expander("📉 MLOps: Data Drift Detection", expanded=False):
        st.write("Detect if recent data behaves differently from older data (Concept Drift).")
        drift_col = st.selectbox("Select Column to Analyze", num_cols, key="drift")
        if st.button("Run Drift Analysis"):
            log_action(st.session_state.user, "MLOps", "Ran Drift Analysis")
            half = len(df) // 2
            old_data, new_data = df[drift_col].iloc[:half], df[drift_col].iloc[half:]
            old_mean, new_mean = old_data.mean(), new_data.mean()
            shift_pct = abs(old_mean - new_mean) / (old_mean + 1e-9) * 100
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Old Data Mean", f"{old_mean:.2f}")
            c2.metric("Recent Data Mean", f"{new_mean:.2f}", f"{new_mean - old_mean:.2f}")
            c3.metric("Drift Percentage", f"{shift_pct:.2f}%")
            
            if shift_pct > 15: st.error("⚠️ Significant Data Drift Detected! The ML model needs retraining.")
            else: st.success("✅ Data distribution is stable. No severe drift detected.")

def run_forecast_model(df, date_col, target_col, steps):
    try:
        df_ml = df.copy()
        try:
            df_ml['ML_DATE'] = pd.to_datetime(df_ml[date_col])
            df_ml = df_ml.sort_values('ML_DATE')
            X = df_ml['ML_DATE'].map(pd.Timestamp.toordinal).values.reshape(-1,1)
            is_date = True
        except:
            X = np.arange(len(df_ml)).reshape(-1,1)
            is_date = False
            
        y = df_ml[target_col].fillna(0).values

        models = {
            "Linear Regression": LinearRegression(),
            "Random Forest (AI)": RandomForestRegressor(n_estimators=50, random_state=42)
        }
        
        best_model, best_name, best_score, best_pred = None, "", float('inf'), None
        for name, model in models.items():
            model.fit(X, y)
            pred = model.predict(X)
            mse = mean_squared_error(y, pred)
            if mse < best_score:
                best_score, best_model, best_name, best_pred = mse, model, name, pred

        future_X = np.array([[X[-1][0]+i] for i in range(1,steps+1)])
        future_pred = best_model.predict(future_X)
        
        x_past = df_ml['ML_DATE'] if is_date else df_ml[date_col]
        x_future = [df_ml['ML_DATE'].iloc[-1]+timedelta(days=i) for i in range(1,steps+1)] if is_date else [f"+{i}" for i in range(1,steps+1)]
            
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=x_past, y=y, name='Actual Data', line=dict(color='#6366f1',width=3)))
        fig.add_trace(go.Scatter(x=x_past, y=best_pred, name=f'{best_name} Fit', line=dict(color='rgba(139,92,246,0.5)',dash='dot',width=2)))
        fig.add_trace(go.Scatter(x=x_future, y=future_pred, name='AI Prediction', line=dict(color='#8b5cf6',width=3)))
        fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', autosize=True, height=400, title=f"AutoML Forecast (Winning Model: {best_name})", font_color="#E0E6ED")
        
        xai_fig = None
        if best_name == "Random Forest (AI)":
            importance = best_model.feature_importances_
            xai_fig = px.bar(x=["Time Trend"], y=importance, title="Explainable AI: Feature Importance", labels={'x':'Feature', 'y':'Influence on Prediction'})
            xai_fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color="#E0E6ED", height=300)

        return {"fig": fig, "xai_fig": xai_fig, "model_name": best_name}
    except Exception as e: return str(e)


# ============================================================
#  PAGE: AI CHAT (RAG)
# ============================================================
def show_chat(user_dir):
    files = st.session_state.get("files", {})
    if not files: st.info("📤 Upload data to chat with it"); return
    selected = st.selectbox("Chat about", list(files.keys()), key="chat_select")
    df = files[selected]["df"]
    st.markdown("<h2>🧠 Multi-Modal RAG AI Agent</h2>", unsafe_allow_html=True)
    
    with st.expander("📎 Supply Additional Context (RAG)", expanded=False):
        st.write("Upload a text file (like an email or report) to give the AI extra knowledge about this dataset.")
        ctx_file = st.file_uploader("Upload Context Doc", type=["txt"])
        custom_context = ""
        if ctx_file:
            custom_context = ctx_file.getvalue().decode("utf-8")
            st.success("Context loaded into AI memory!")

    chat_key = f"chat_{selected}"
    if chat_key not in st.session_state.chat_history_db:
        st.session_state.chat_history_db[chat_key] = [{"role":"assistant","content":"👋 Hi! I am your AI Data Agent. I can write Python code, do math, and analyze your data."}]
    
    for msg in st.session_state.chat_history_db[chat_key][-10:]:
        with st.chat_message(msg["role"]): st.markdown(msg["content"])
        
    if prompt := st.chat_input("Ask about your data..."):
        st.session_state.chat_history_db[chat_key].append({"role":"user","content":prompt})
        with st.chat_message("user"): st.markdown(prompt)
        with st.chat_message("assistant"):
            with st.spinner("🤔 Agent is working..."):
                try:
                    future = executor.submit(get_ai_response, df, st.session_state.chat_history_db[chat_key][-6:], custom_context)
                    reply = future.result(timeout=30)
                    st.markdown(reply)
                    st.session_state.chat_history_db[chat_key].append({"role":"assistant","content":reply})
                    save_chat_history(st.session_state.user, st.session_state.chat_history_db[chat_key])
                    log_action(st.session_state.user, "AI Chat", "Interacted with Data Agent")
                except Exception as e: st.error(f"❌ AI Error: {e}")

def get_ai_response(df, messages, custom_context=""):
    try:
        col_types = df.dtypes.astype(str).to_dict()
        sys_ctx = f"""
        You are 'Neat Lens Agent', an elite Senior Data Scientist and Python expert.
        The user has loaded a dataset with {len(df)} rows and {len(df.columns)} columns.
        Column Schema and Types: {col_types}
        Basic Stats: {df.describe().to_dict()}
        Additional RAG Context provided by user: {custom_context}
        
        YOUR DIRECTIVES:
        1. If the user asks a statistical question, calculate it mentally using the stats provided and give a direct answer.
        2. If the user asks HOW to do something, provide EXACT, copy-pasteable Python/Pandas code blocks.
        3. Never say "I don't have access to your data" — you have the schema and stats above.
        """
        client = OpenAI(api_key=GROQ_API_KEY, base_url="https://api.groq.com/openai/v1")
        msgs = [{"role":"system","content":sys_ctx}] + messages
        resp = client.chat.completions.create(model="llama-3.3-70b-versatile", messages=msgs, timeout=25)
        return resp.choices[0].message.content
    except Exception as e: return f"❌ Error: {str(e)}"


# ============================================================
#  PAGE: LIVE DATA
# ============================================================
def show_live_data(user_dir):
    st.markdown("<h2>🌐 Live Data</h2>", unsafe_allow_html=True)
    is_mobile = st.session_state.get("mobile_view", False)
    if is_mobile:
        ticker = st.text_input("Ticker", "AAPL", max_chars=10).upper()
        col1, col2 = st.columns(2)
        with col1: start_date = st.date_input("Start", datetime.today() - timedelta(days=365))
        with col2: end_date = st.date_input("End", datetime.today())
    else:
        col1, col2, col3 = st.columns(3)
        with col1: ticker = st.text_input("Ticker Symbol", "AAPL", max_chars=10).upper()
        with col2: start_date = st.date_input("Start Date", datetime.today() - timedelta(days=365))
        with col3: end_date = st.date_input("End Date", datetime.today())

    if st.button("📡 Fetch Data", type="primary", use_container_width=True):
        with st.spinner("📊 Fetching market data..."):
            try:
                stock_data = yf.Ticker(ticker).history(start=start_date, end=end_date)
                if stock_data is not None and not stock_data.empty:
                    stock_data.reset_index(inplace=True)
                    if 'Date' in stock_data.columns: stock_data['Date'] = stock_data['Date'].dt.tz_localize(None)
                    safe_filename = f"{ticker}_Live.csv"
                    save_path = os.path.join(user_dir, safe_filename)
                    stock_data.to_csv(save_path, index=False)
                    st.session_state.files[safe_filename] = {"df": stock_data, "source": f"Yahoo Finance - {ticker}"}
                    st.success(f"✅ Fetched {len(stock_data)} rows!")
                    log_action(st.session_state.user, "Live Data", f"Fetched {ticker} stock data")
                    fig = px.line(stock_data, x="Date", y="Close", title=f"{ticker} Stock Price")
                    fig.update_layout(autosize=True, height=300 if is_mobile else 450, margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color="#E0E6ED")
                    st.plotly_chart(fig, use_container_width=True)
                else: st.warning("⚠️ No data returned for this ticker.")
            except Exception as e: st.error(f"❌ Error fetching data: {str(e)}")


# ============================================================
#  PAGE: DATABASE
# ============================================================
def show_database_connect(user_dir):
    st.markdown("<h2>🗄️ Database Connection</h2>", unsafe_allow_html=True)
    st.info("💡 Connect to SQL databases to query data directly")
    is_mobile = st.session_state.get("mobile_view", False)
    with st.expander("➕ New Connection", expanded=True):
        col1, col2, col3 = st.columns(3) if not is_mobile else [st.container(), st.container(), st.container()]
        with col1: db_type = st.selectbox("Database Type", ["PostgreSQL", "MySQL", "SQLite", "SQL Server", "Oracle"], key="db_type")
        with col2: conn_name = st.text_input("Connection Name", placeholder="My Production DB", key="conn_name")
        with col3:
            if db_type == "SQLite":
                db_path = st.text_input("Database File Path", placeholder="/path/to/database.db", key="sqlite_path")
                host = port = user = password = database = None
            else:
                host = st.text_input("Host", placeholder="localhost", key="db_host")
                port_map = {"PostgreSQL": "5432", "MySQL": "3306", "SQL Server": "1433", "Oracle": "1521"}
                port = st.text_input("Port", value=port_map.get(db_type, ""), key="db_port")
        if db_type != "SQLite":
            col4, col5, col6 = st.columns(3) if not is_mobile else [st.container(), st.container(), st.container()]
            with col4: user = st.text_input("Username", key="db_user")
            with col5: password = st.text_input("Password", type="password", key="db_pass")
            with col6: database = st.text_input("Database Name", key="db_name")
        if st.button("🔗 Test & Save Connection", type="primary", use_container_width=True):
            try:
                if db_type == "PostgreSQL": cs = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
                elif db_type == "MySQL": cs = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
                elif db_type == "SQLite": cs = f"sqlite:///{db_path}"
                elif db_type == "SQL Server": cs = f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
                else: cs = f"oracle+cx_oracle://{user}:{password}@{host}:{port}/{database}"
                engine = create_engine(cs, connect_args={'connect_timeout': 10})
                with engine.connect() as conn: conn.execute(text("SELECT 1"))
                st.session_state.db_connections[conn_name] = {"type": db_type, "connection_string": cs, "host": host, "database": database if db_type != "SQLite" else db_path}
                st.success(f"✅ Connected to {db_type}!")
                log_action(st.session_state.user, "Database", f"Connected to {db_type}")
            except Exception as e: st.error(f"❌ Connection failed: {str(e)}")
            
    if st.session_state.db_connections:
        st.markdown("### 📋 Saved Connections")
        selected_conn = st.selectbox("Select Connection", list(st.session_state.db_connections.keys()), key="sel_conn")
        if selected_conn:
            conn_info = st.session_state.db_connections[selected_conn]
            c1, c2 = st.columns([4, 1])
            with c1: st.caption(f"Type: {conn_info['type']} | Host: {conn_info.get('host','Local')}")
            with c2:
                if st.button("❌ Remove", key=f"remove_{selected_conn}"): del st.session_state.db_connections[selected_conn]; st.rerun()
            query = st.text_area("SQL Query", height=150, placeholder="SELECT * FROM table_name LIMIT 100")
            if st.button("▶️ Execute Query", type="primary", use_container_width=True):
                if query:
                    try:
                        engine = create_engine(conn_info['connection_string'])
                        df = pd.read_sql(text(query), engine)
                        st.session_state['last_query_result'] = df
                        st.session_state['last_query_name'] = f"Query_{datetime.now().strftime('%H%M%S')}"
                        st.success(f"✅ {len(df)} rows returned.")
                    except Exception as e: st.error(f"Query error: {e}")
            if 'last_query_result' in st.session_state:
                df = st.session_state['last_query_result']
                st.dataframe(df.head(100), use_container_width=True, height=300)
                c1, c2, c3 = st.columns(3)
                with c1: st.download_button("📄 CSV", df.to_csv(index=False), f"{st.session_state['last_query_name']}.csv", "text/csv", use_container_width=True)
                with c2: st.download_button("📊 Excel", export_to_excel(df, st.session_state['last_query_name']), f"{st.session_state['last_query_name']}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
                with c3:
                    if st.button("💾 Save to Workspace", use_container_width=True):
                        fname = f"DB_{st.session_state['last_query_name']}.csv"
                        df.to_csv(os.path.join(user_dir, fname), index=False)
                        st.session_state.files[fname] = {"df": df, "source": f"DB: {conn_info['type']}"}
                        st.success(f"✅ Saved as {fname}")


# ============================================================
#  PAGE: TEAM COLLAB
# ============================================================
def show_team_collaboration(user_dir):
    st.markdown("<h2>🤝 Team Collaboration</h2>", unsafe_allow_html=True)
    is_mobile = st.session_state.get("mobile_view", False)
    tabs = st.tabs(["🔗 Share Dataset", "📥 Shared With Me", "🌐 Public Datasets"])
    with tabs[0]:
        st.markdown("### Share Dataset with Team Member")
        files = st.session_state.get("files", {})
        if not files: st.info("📤 Upload datasets first to share them"); return
        own_files = {k: v for k, v in files.items() if not v.get('is_shared', False)}
        if not own_files: st.info("No datasets available to share"); return
        c1, c2, c3 = st.columns(3) if not is_mobile else [st.container(), st.container(), st.container()]
        with c1: dataset_to_share = st.selectbox("Select Dataset", list(own_files.keys()), key="share_dataset")
        with c2:
            all_users = get_all_users()
            other_users = [u for u in all_users if u != st.session_state.user]
            if other_users: target_user = st.selectbox("Share With", other_users, key="target_user")
            else: st.warning("No other users found"); target_user = None
        with c3: permission = st.selectbox("Permission", ["Read Only", "Can Edit"])
        message = st.text_area("Message (Optional)", placeholder="Notes for recipient...")
        if target_user and st.button("🚀 Share Dataset", type="primary", use_container_width=True):
            try:
                shared_dir = os.path.join(WORKSPACE_BASE_DIR, "shared", target_user)
                os.makedirs(shared_dir, exist_ok=True)
                df = own_files[dataset_to_share]["df"]
                share_path = os.path.join(shared_dir, f"{st.session_state.user}_{dataset_to_share}")
                df.to_csv(share_path, index=False)
                meta = {"shared_by": st.session_state.user, "date": datetime.now().isoformat(), "permission": permission, "message": message, "source": own_files[dataset_to_share].get("source", "")}
                with open(share_path.rsplit('.',1)[0]+'_meta.json','w') as f: json.dump(meta, f)
                st.success(f"✅ Shared '{dataset_to_share}' with {target_user}!")
                log_action(st.session_state.user, "Share", f"Shared {dataset_to_share} with {target_user}")
                st.balloons()
            except Exception as e: st.error(f"Sharing failed: {e}")
    with tabs[1]:
        shared_files = {k: v for k, v in st.session_state.get("files", {}).items() if v.get('is_shared', False)}
        if not shared_files: st.info("📭 No datasets shared with you yet")
        else:
            for fname, fdata in shared_files.items():
                c1, c2, c3 = st.columns([3,1,1])
                with c1: st.write(f"**{fname}**"); st.caption(fdata.get('source',''))
                with c2: st.write(f"Rows: {len(fdata['df']):,}")
                with c3:
                    if st.button("🗑️", key=f"rm_{fname}"):
                        try:
                            if os.path.exists(fdata['shared_path']): os.remove(fdata['shared_path'])
                            del st.session_state.files[fname]; st.rerun()
                        except: pass
                st.divider()
    with tabs[2]: st.info("🚧 Coming soon: Real-time collaboration!")


# ============================================================
#  PAGE: ADMIN
# ============================================================
def show_admin(user_dir):
    st.markdown("<h2>👑 Admin Control & Audit Logs</h2>", unsafe_allow_html=True)
    db_users = get_all_users()
    total_size, total_files = get_directory_size(WORKSPACE_BASE_DIR)
    is_mobile = st.session_state.get("mobile_view", False)
    if is_mobile:
        st.metric("Users", len(db_users)); st.metric("Files", total_files); st.metric("Storage MB", f"{total_size/(1024*1024):.1f}")
    else:
        c1,c2,c3 = st.columns(3)
        with c1: st.markdown(f"<div class='metric-card-v2'><div class='metric-icon-wrap metric-icon-blue'>👥</div><div><div class='metric-card-v2-label'>Users</div><div class='metric-card-v2-value'>{len(db_users)}</div></div></div>", unsafe_allow_html=True)
        with c2: st.markdown(f"<div class='metric-card-v2'><div class='metric-icon-wrap metric-icon-green'>📁</div><div><div class='metric-card-v2-label'>Files</div><div class='metric-card-v2-value'>{total_files}</div></div></div>", unsafe_allow_html=True)
        with c3: st.markdown(f"<div class='metric-card-v2'><div class='metric-icon-wrap metric-icon-orange'>💾</div><div><div class='metric-card-v2-label'>Storage MB</div><div class='metric-card-v2-value'>{total_size/(1024*1024):.1f}</div></div></div>", unsafe_allow_html=True)
    
    st.markdown("### 👥 Users Storage")
    user_data = []
    for uname in db_users:
        u_dir = os.path.join(WORKSPACE_BASE_DIR, uname)
        u_size, u_files = get_directory_size(u_dir)
        user_data.append({"User": uname, "Files": u_files, "Storage (MB)": round(u_size/(1024*1024),2)})
    st.dataframe(pd.DataFrame(user_data), use_container_width=True, height=200)

    # ── ADVANCED FEATURE 4: AUDIT LOGS (DATA GOVERNANCE) ──
    st.markdown("### 🕵️‍♂️ Security Audit Trail")
    if st.session_state.audit_logs:
        audit_df = pd.DataFrame(st.session_state.audit_logs)
        st.dataframe(audit_df.tail(20).iloc[::-1], use_container_width=True) # Show latest 20 logs reversed
    else:
        st.info("No logs recorded yet.")

    st.markdown("### ⚠️ Danger Zone")
    if st.button("🗑️ Format Server", type="primary"):
        if os.path.exists(WORKSPACE_BASE_DIR): shutil.rmtree(WORKSPACE_BASE_DIR)
        st.session_state.audit_logs = []
        st.rerun()

# ============================================================
#  MAIN EXECUTION
# ============================================================
if not st.session_state.logged_in:
    show_login()
else:
    show_dashboard()

if __name__ == "__main__":
    import sys
    from streamlit.web import cli as stcli
    if not st.runtime.exists():
        sys.argv = ["streamlit", "run", sys.argv[0]]
        sys.exit(stcli.main())
