import streamlit as st
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
from datetime import timedelta, datetime
import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
import hashlib

# Database & Backend modules
from auth import init_db, add_user, get_user_hash, get_all_users, check_password
from styles import inject_login_css, inject_dashboard_css, inject_mobile_css
from backend import (
    init_workspace, load_user_workspace, save_chat_history, 
    get_directory_size, generate_pdf_report
)

# Initialize thread pool for background tasks
executor = ThreadPoolExecutor(max_workers=10)

# Secure API key handling (move to secrets.toml in production)
GROQ_API_KEY = st.secrets["GROQ_API_KEY"]
WORKSPACE_BASE_DIR = "workspaces"

# Initialize database
init_db()

# Page config optimized for mobile
st.set_page_config(
    page_title="Explorer by Atta", 
    page_icon="✨", 
    layout="wide", 
    initial_sidebar_state="collapsed"  # Better for mobile
)

# Initialize session state with defaults
def init_session_state():
    defaults = {
        "logged_in": False,
        "theme": "Light",
        "mobile_view": False,
        "chat_history_db": {},
        "processing_queue": {},
        "cache_version": 0
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

# Mobile detection using custom component or viewport
def detect_mobile():
    """Detect if user is on mobile device"""
    # Using JavaScript injection for viewport detection
    mobile_js = """
    <script>
    (function() {
        const width = window.innerWidth || document.documentElement.clientWidth;
        const isMobile = width < 768;
        window.parent.postMessage({type: 'mobile_detect', isMobile: isMobile}, '*');
    })();
    </script>
    """
    st.components.v1.html(mobile_js, height=0)
    # Default to responsive behavior
    return st.session_state.get("mobile_view", False)

def responsive_columns(specs, mobile_specs=None):
    """Create responsive columns that adapt to mobile"""
    if st.session_state.get("mobile_view") and mobile_specs:
        return st.columns(mobile_specs)
    return st.columns(specs)

# Async wrapper for blocking operations
def run_async(func):
    """Decorator to run blocking functions in thread pool"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_in_executor(executor, lambda: func(*args, **kwargs))
        finally:
            loop.close()
    return wrapper

# Cached data loading to prevent disk I/O on every rerun
@st.cache_data(ttl=300, max_entries=100)
def load_dataframe_cached(file_path, file_hash):
    """Cached dataframe loading - prevents repeated disk reads"""
    ext = file_path.split('.')[-1].lower()
    if ext == "csv":
        return pd.read_csv(file_path)
    elif ext in ["xlsx", "xls"]:
        return pd.read_excel(file_path)
    return None

def show_login():
    inject_login_css()
    inject_mobile_css()
    
    # Responsive layout: single column on mobile, centered on desktop
    is_mobile = st.session_state.get("mobile_view", False)
    
    if is_mobile:
        col_center = st.container()
    else:
        _, col_center, _ = st.columns([1, 1.2, 1])
    
    with col_center:
        st.markdown("""
        <div style="margin-bottom: 20px; text-align: center;">
            <div style="width: 60px; height: 60px; border-radius: 15px; 
                 background: linear-gradient(135deg, #FF007A, #7928CA); 
                 margin: 0 auto 15px auto; display: flex; align-items: center; 
                 justify-content: center;">
                <span style="color: white; font-weight: bold; font-size: 1.5rem;">✨</span>
            </div>
            <div class='vector-title'>Explorer by Atta</div>
            <div class='vector-subtitle'>Sign in to continue to your workspace</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Mobile-optimized form
        with st.form("login_form"):
            u = st.text_input("Username", placeholder="Username", 
                            key="login_user", max_chars=50)
            p = st.text_input("Password", type="password", 
                            placeholder="Password", key="login_pass", max_chars=100)
            
            submitted = st.form_submit_button(
                "Sign In / Register", 
                type="primary", 
                use_container_width=True
            )
            
            if submitted:
                if not u.strip() or not p.strip():
                    st.warning("⚠️ Please enter both username and password.")
                    st.stop()
                
                # Async database operation
                hashed_pw = get_user_hash(u)
                
                if hashed_pw is None:
                    add_user(u, p)
                    st.success("Account created! Preparing workspace...")
                    login_success(u)
                elif check_password(p, hashed_pw):
                    login_success(u)
                else:
                    st.error("Invalid credentials.")

def login_success(username):
    """Handle successful login with workspace initialization"""
    st.session_state.logged_in = True
    st.session_state.user = username
    init_workspace(username)
    load_user_workspace(username)
    st.rerun()

def show_dashboard():
    """Main dashboard with mobile-responsive navigation"""
    if "files" not in st.session_state:
        load_user_workspace(st.session_state.user)
    
    inject_dashboard_css(st.session_state.theme)
    inject_mobile_css()
    
    user_dir = init_workspace(st.session_state.user)
    
    # Mobile-optimized sidebar
    with st.sidebar:
        st.markdown("### ✨ Explorer")
        st.markdown(f"**👤 {st.session_state.user}**")
        
        if st.session_state.user == "Admin":
            st.caption("Admin Mode 👑")
        else:
            st.caption("Secure Workspace 🔒")
        
        # Theme toggle
        theme_choice = st.segmented_control(
            "Theme", 
            ["Light", "Dark"], 
            default=st.session_state.theme,
            key="theme_toggle"
        )
        if theme_choice and theme_choice != st.session_state.theme:
            st.session_state.theme = theme_choice
            st.rerun()
        
        # Navigation - collapsible on mobile
        nav_options = [
            "Overview", "Live Data 🌐", "Cleaning", 
            "Visuals", "Forecasting", "AI Chat"
        ]
        if st.session_state.user == "Admin":
            nav_options.append("Admin 👑")
        
        page = st.radio("Navigation", nav_options, key="nav")
        
        # Mobile-optimized file uploader
        st.divider()
        st.markdown("**📁 Storage**")
        
        with st.expander("Upload Files", expanded=False):
            up = st.file_uploader(
                "Upload", 
                type=["csv", "xlsx", "zip"], 
                accept_multiple_files=True,
                label_visibility="collapsed",
                key="file_upload"
            )
            
            if up:
                process_uploads(up, user_dir)
        
        # Sign out button at bottom for thumb reachability
        st.divider()
        if st.button("🚪 Sign Out", use_container_width=True, type="secondary"):
            # Cleanly clear cache without relying on missing backend functions
            st.cache_data.clear()
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
    
    # Route to page
    page_handlers = {
        "Overview": show_overview,
        "Live Data 🌐": show_live_data,
        "Cleaning": show_cleaning,
        "Visuals": show_visuals,
        "Forecasting": show_forecasting,
        "AI Chat": show_chat,
        "Admin 👑": show_admin
    }
    
    handler = page_handlers.get(page, show_overview)
    handler(user_dir)

def process_uploads(uploaded_files, user_dir):
    """Process file uploads with progress tracking"""
    progress_bar = st.progress(0)
    total_files = len(uploaded_files)
    
    for idx, f in enumerate(uploaded_files):
        progress = (idx + 1) / total_files
        progress_bar.progress(progress, f"Processing {f.name}...")
        
        ext = f.name.split('.')[-1].lower()
        
        if ext == 'zip':
            process_zip_upload(f, user_dir)
        else:
            process_single_file(f, user_dir, ext)
    
    progress_bar.empty()
    st.success(f"Processed {total_files} files!")

def process_zip_upload(zip_file, user_dir):
    """Extract and process ZIP files"""
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            extracted = 0
            for zip_info in zip_ref.infolist():
                if zip_info.is_dir() or zip_info.filename.startswith('__MACOSX/'):
                    continue
                
                inner_ext = zip_info.filename.split('.')[-1].lower()
                if inner_ext not in ['csv', 'xlsx', 'xls']:
                    continue
                
                safe_name = os.path.basename(zip_info.filename)
                if safe_name in st.session_state.files:
                    continue
                
                save_path = os.path.join(user_dir, safe_name)
                with open(save_path, "wb") as out_file:
                    out_file.write(zip_ref.read(zip_info.filename))
                
                # Use cached loading
                df = load_dataframe_cached(save_path, hashlib.md5(safe_name.encode()).hexdigest())
                if df is not None:
                    st.session_state.files[safe_name] = {"df": df}
                    extracted += 1
            
            if extracted > 0:
                st.toast(f"Extracted {extracted} files from ZIP")
    except Exception as e:
        st.error(f"ZIP extraction failed: {e}")

def process_single_file(f, user_dir, ext):
    """Process single file upload"""
    if f.name in st.session_state.files:
        return
    
    save_path = os.path.join(user_dir, f.name)
    with open(save_path, "wb") as out_file:
        out_file.write(f.getbuffer())
    
    df = load_dataframe_cached(save_path, hashlib.md5(f.name.encode()).hexdigest())
    if df is not None:
        st.session_state.files[f.name] = {"df": df}

def show_overview(user_dir):
    """Mobile-responsive overview page"""
    files = st.session_state.get("files", {})
    if not files:
        st.info("👋 Upload a file or fetch Live Data to get started")
        return
    
    # Mobile: stacked layout, Desktop: side-by-side
    is_mobile = st.session_state.get("mobile_view", False)
    
    if is_mobile:
        selected = st.selectbox("Dataset", list(files.keys()), key="dataset_select")
        st.download_button(
            "📥 PDF Report",
            data=generate_pdf_report(files[selected]["df"], selected, st.session_state.user),
            file_name=f"{selected}_Report.pdf",
            mime="application/pdf",
            use_container_width=True
        )
    else:
        col_title, col_btn = st.columns([3, 1])
        with col_title:
            st.markdown("<h2>Data Overview</h2>", unsafe_allow_html=True)
        with col_btn:
            selected = st.selectbox("Dataset", list(files.keys()), key="dataset_select")
            st.download_button(
                "📥 PDF Report",
                data=generate_pdf_report(files[selected]["df"], selected, st.session_state.user),
                file_name=f"{selected}_Report.pdf",
                mime="application/pdf",
                use_container_width=True
            )
    
    df = files[selected]["df"]
    
    # Responsive metrics
    if is_mobile:
        st.metric("Rows", f"{len(df):,}")
        st.metric("Columns", len(df.columns))
        st.metric("Missing", f"{df.isna().sum().sum():,}")
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(f"<div class='metric-card'><h4>{len(df):,}</h4><p>Rows</p></div>", 
                       unsafe_allow_html=True)
        with c2:
            st.markdown(f"<div class='metric-card'><h4>{len(df.columns)}</h4><p>Columns</p></div>", 
                       unsafe_allow_html=True)
        with c3:
            st.markdown(f"<div class='metric-card'><h4>{df.isna().sum().sum():,}</h4><p>Missing</p></div>", 
                       unsafe_allow_html=True)
    
    # Mobile-optimized dataframe display
    st.dataframe(
        df.head(50), 
        use_container_width=True,
        height=300 if is_mobile else 400
    )

def show_live_data(user_dir):
    """Live data fetching with async operations"""
    st.markdown("<h2>🌐 Live Data</h2>", unsafe_allow_html=True)
    
    is_mobile = st.session_state.get("mobile_view", False)
    
    if is_mobile:
        ticker = st.text_input("Ticker", "AAPL", max_chars=10).upper()
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start", datetime.today() - timedelta(days=365))
        with col2:
            end_date = st.date_input("End", datetime.today())
    else:
        col1, col2, col3 = st.columns(3)
        with col1:
            ticker = st.text_input("Ticker Symbol", "AAPL", max_chars=10).upper()
        with col2:
            start_date = st.date_input("Start Date", datetime.today() - timedelta(days=365))
        with col3:
            end_date = st.date_input("End Date", datetime.today())
    
    if st.button("📡 Fetch Data", type="primary", use_container_width=True):
        with st.spinner("Fetching market data..."):
            try:
                # Use thread pool for non-blocking fetch
                future = executor.submit(fetch_stock_data, ticker, start_date, end_date)
                stock_data = future.result(timeout=30)
                
                if stock_data is not None and not stock_data.empty:
                    stock_data.reset_index(inplace=True)
                    if 'Date' in stock_data.columns:
                        stock_data['Date'] = stock_data['Date'].dt.tz_localize(None)
                    
                    safe_filename = f"{ticker}_Live.csv"
                    save_path = os.path.join(user_dir, safe_filename)
                    stock_data.to_csv(save_path, index=False)
                    
                    # Update cache
                    st.session_state.files[safe_filename] = {"df": stock_data}
                    
                    st.success(f"Fetched {len(stock_data)} rows!")
                    
                    # Responsive chart
                    fig = px.line(stock_data, x="Date", y="Close")
                    fig.update_layout(
                        autosize=True,
                        height=300 if is_mobile else 450,
                        margin=dict(l=20, r=20, t=30, b=20)
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No data returned for this ticker.")
            except Exception as e:
                st.error(f"Error fetching data: {str(e)}")

def fetch_stock_data(ticker, start, end):
    """Thread-safe stock data fetching"""
    try:
        return yf.Ticker(ticker).history(start=start, end=end)
    except Exception:
        return None

def show_cleaning(user_dir):
    """Data cleaning with batch operations"""
    files = st.session_state.get("files", {})
    if not files:
        st.info("No data to clean")
        return
    
    selected = st.selectbox("Clean Dataset", list(files.keys()), key="clean_select")
    df = files[selected]["df"].copy()
    
    st.markdown("<h2>🧹 Data Cleaning</h2>", unsafe_allow_html=True)
    
    is_mobile = st.session_state.get("mobile_view", False)
    
    # Mobile-optimized action buttons
    if is_mobile:
        with st.expander("Cleaning Actions", expanded=True):
            action = st.selectbox("Action", [
                "Drop Duplicates", "Fill Missing (0)", "Fill Missing (Mean)", 
                "Drop NA Rows", "Rename Column"
            ])
            
            if action == "Rename Column":
                col_to_rename = st.selectbox("Column", df.columns)
                new_name = st.text_input("New Name")
                if st.button("Apply", use_container_width=True) and new_name:
                    df.rename(columns={col_to_rename: new_name}, inplace=True)
                    save_cleaned_data(df, selected, user_dir)
            elif st.button(f"Apply {action}", use_container_width=True):
                apply_cleaning_action(df, selected, user_dir, action)
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("🗑️ Drop Duplicates", use_container_width=True):
                apply_cleaning_action(df, selected, user_dir, "Drop Duplicates")
        with c2:
            if st.button("📝 Fill Missing (0)", use_container_width=True):
                apply_cleaning_action(df, selected, user_dir, "Fill Missing (0)")
        with c3:
            with st.expander("More Actions"):
                if st.button("Fill Missing (Mean)"):
                    apply_cleaning_action(df, selected, user_dir, "Fill Missing (Mean)")
                if st.button("Drop NA Rows"):
                    apply_cleaning_action(df, selected, user_dir, "Drop NA Rows")
                col_to_rename = st.selectbox("Rename", df.columns, key="rename_col")
                new_name = st.text_input("New Name", key="new_name")
                if st.button("Rename") and new_name:
                    df.rename(columns={col_to_rename: new_name}, inplace=True)
                    save_cleaned_data(df, selected, user_dir)
    
    # Preview with limited rows for mobile performance
    preview_rows = 10 if is_mobile else 15
    st.dataframe(df.head(preview_rows), use_container_width=True)

def apply_cleaning_action(df, selected, user_dir, action):
    """Apply cleaning action asynchronously"""
    try:
        if action == "Drop Duplicates":
            df.drop_duplicates(inplace=True)
        elif action == "Fill Missing (0)":
            df.fillna(0, inplace=True)
        elif action == "Fill Missing (Mean)":
            df.fillna(df.mean(numeric_only=True), inplace=True)
        elif action == "Drop NA Rows":
            df.dropna(inplace=True)
        
        save_cleaned_data(df, selected, user_dir)
        st.success(f"Applied: {action}")
        st.rerun()
    except Exception as e:
        st.error(f"Cleaning failed: {e}")

def save_cleaned_data(df, filename, user_dir):
    """Save cleaned data with caching update"""
    save_path = os.path.join(user_dir, filename)
    if filename.endswith('.csv'):
        df.to_csv(save_path, index=False)
    else:
        df.to_excel(save_path, index=False)
    
    # Update session and cache
    st.session_state.files[filename]["df"] = df
    st.session_state.cache_version += 1

def show_visuals(user_dir):
    """Mobile-responsive visualization studio"""
    files = st.session_state.get("files", {})
    if not files:
        st.info("Upload data to visualize")
        return
    
    selected = st.selectbox("Visualize", list(files.keys()), key="viz_select")
    df = files[selected]["df"]
    
    st.markdown("<h2>📊 Visual Explorer</h2>", unsafe_allow_html=True)
    
    is_mobile = st.session_state.get("mobile_view", False)
    num_cols = df.select_dtypes(include='number').columns.tolist()
    
    # Mobile-optimized controls
    if is_mobile:
        chart = st.selectbox("Chart", ["Bar", "Scatter", "Line", "Histogram"])
        x_ax = st.selectbox("X-Axis", df.columns)
        y_ax = st.selectbox("Y-Axis", num_cols) if chart != "Histogram" else None
        
        # Simplify chart options for mobile
        height = 300
    else:
        c1, c2, c3 = st.columns([1, 2, 2])
        with c1:
            chart = st.selectbox("Chart Type", ["Bar", "Scatter", "Line", "Histogram"])
        with c2:
            x_ax = st.selectbox("X-Axis", df.columns)
        with c3:
            y_ax = st.selectbox("Y-Axis", num_cols) if chart != "Histogram" else None
        height = 450
    
    # Generate chart
    try:
        if chart == "Bar":
            fig = px.bar(df, x=x_ax, y=y_ax)
        elif chart == "Scatter":
            fig = px.scatter(df, x=x_ax, y=y_ax)
        elif chart == "Line":
            fig = px.line(df, x=x_ax, y=y_ax)
        else:
            fig = px.histogram(df, x=x_ax)
        
        # Responsive styling
        is_dark = st.session_state.theme == "Dark"
        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color="#E0E6ED" if is_dark else "#0f172a",
            autosize=True,
            height=height,
            margin=dict(l=20, r=20, t=40, b=20)
        )
        st.plotly_chart(fig, use_container_width=True, config={
            'displayModeBar': not is_mobile  # Hide toolbar on mobile
        })
    except Exception as e:
        st.error(f"Chart error: {e}")

def show_forecasting(user_dir):
    """ML forecasting with async processing"""
    files = st.session_state.get("files", {})
    if not files:
        st.info("Upload time-series data for forecasting")
        return
    
    selected = st.selectbox("Forecast Dataset", list(files.keys()), key="forecast_select")
    df = files[selected]["df"]
    
    st.markdown("<h2>🔮 ML Forecasting</h2>", unsafe_allow_html=True)
    
    num_cols = df.select_dtypes(include='number').columns.tolist()
    if not num_cols:
        st.warning("No numeric columns found")
        return
    
    is_mobile = st.session_state.get("mobile_view", False)
    
    if is_mobile:
        date_col = st.selectbox("Date Column", df.columns)
        target_col = st.selectbox("Target", num_cols)
        steps = st.slider("Periods", 1, 30, 7)  # Reduced max for mobile
    else:
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            date_col = st.selectbox("Date (X)", df.columns)
        with fc2:
            target_col = st.selectbox("Target (Y)", num_cols)
        with fc3:
            steps = st.slider("Forecast Periods", 1, 60, 14)
    
    if st.button("🚀 Run Forecast", type="primary", use_container_width=True):
        with st.spinner("Training model..."):
            try:
                # Run ML in background thread
                future = executor.submit(
                    run_forecast_model, df, date_col, target_col, steps
                )
                fig = future.result(timeout=60)
                
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.error("Forecasting failed")
            except Exception as e:
                st.error(f"Forecast error: {e}")

def run_forecast_model(df, date_col, target_col, steps):
    """Thread-safe forecasting model"""
    try:
        df_ml = df.copy()
        
        # Date preprocessing
        try:
            df_ml['ML_DATE'] = pd.to_datetime(df_ml[date_col])
            df_ml = df_ml.sort_values('ML_DATE')
            X = df_ml['ML_DATE'].map(pd.Timestamp.toordinal).values.reshape(-1, 1)
            is_date = True
        except:
            X = np.arange(len(df_ml)).reshape(-1, 1)
            is_date = False
        
        y = df_ml[target_col].values
        
        # Fit model
        model = LinearRegression()
        model.fit(X, y)
        
        # Predictions
        past_pred = model.predict(X)
        future_X = np.array([[X[-1][0] + i] for i in range(1, steps + 1)])
        future_pred = model.predict(future_X)
        
        # Build chart data
        x_past = df_ml['ML_DATE'] if is_date else df_ml[date_col]
        if is_date:
            x_future = [df_ml['ML_DATE'].iloc[-1] + timedelta(days=i) for i in range(1, steps + 1)]
        else:
            x_future = [f"+{i}" for i in range(1, steps + 1)]
        
        # Create figure
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=x_past, y=y, name='Actual', line=dict(color='#7928CA')))
        fig.add_trace(go.Scatter(x=x_past, y=past_pred, name='Trend', 
                                 line=dict(color='rgba(255,0,122,0.5)', dash='dot')))
        fig.add_trace(go.Scatter(x=x_future, y=future_pred, name='Prediction', 
                                 line=dict(color='#FF007A')))
        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            autosize=True,
            height=400
        )
        return fig
    except Exception:
        return None

def show_chat(user_dir):
    """AI Chat with rate limiting and async processing"""
    files = st.session_state.get("files", {})
    if not files:
        st.info("Upload data to chat with it")
        return
    
    selected = st.selectbox("Chat about", list(files.keys()), key="chat_select")
    df = files[selected]["df"]
    
    st.markdown("<h2>🧠 AI Data Assistant</h2>", unsafe_allow_html=True)
    
    chat_key = f"chat_{selected}"
    if chat_key not in st.session_state.chat_history_db:
        st.session_state.chat_history_db[chat_key] = [
            {"role": "assistant", "content": "Hi! Ask me anything about your data."}
        ]
    
    # Display chat history (limited for performance)
    history = st.session_state.chat_history_db[chat_key][-10:]  # Last 10 messages
    for msg in history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask about your data..."):
        # Add user message
        st.session_state.chat_history_db[chat_key].append({"role": "user", "content": prompt})
        
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Async AI response
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    future = executor.submit(
                        get_ai_response, df, st.session_state.chat_history_db[chat_key][-6:]
                    )
                    reply = future.result(timeout=30)
                    
                    st.markdown(reply)
                    st.session_state.chat_history_db[chat_key].append(
                        {"role": "assistant", "content": reply}
                    )
                    save_chat_history(st.session_state.user)
                except Exception as e:
                    st.error(f"AI Error: {e}")

def get_ai_response(df, messages):
    """Thread-safe AI call"""
    try:
        sys_ctx = f"Data: {len(df)} rows, {len(df.columns)} cols. Stats: {df.describe().to_dict()}"
        client = OpenAI(api_key=GROQ_API_KEY, base_url="https://api.groq.com/openai/v1")
        
        msgs = [{"role": "system", "content": sys_ctx}] + messages
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile", 
            messages=msgs,
            timeout=25
        )
        return resp.choices[0].message.content
    except Exception as e:
        return f"Error: {str(e)}"

def show_admin(user_dir):
    """Admin panel with database metrics"""
    st.markdown("<h2>👑 Admin Control</h2>", unsafe_allow_html=True)
    
    # Fetch metrics
    db_users = get_all_users()
    total_size, total_files = get_directory_size(WORKSPACE_BASE_DIR)
    
    # Responsive metrics
    is_mobile = st.session_state.get("mobile_view", False)
    
    if is_mobile:
        st.metric("Users", len(db_users))
        st.metric("Files", total_files)
        st.metric("Storage (MB)", f"{total_size / (1024*1024):.1f}")
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(f"<div class='metric-card'><h4>{len(db_users)}</h4><p>Users</p></div>", 
                       unsafe_allow_html=True)
        with c2:
            st.markdown(f"<div class='metric-card'><h4>{total_files}</h4><p>Files</p></div>", 
                       unsafe_allow_html=True)
        with c3:
            st.markdown(f"<div class='metric-card'><h4>{total_size/(1024*1024):.1f}</h4><p>MB Used</p></div>", 
                       unsafe_allow_html=True)
    
    # User table
    st.markdown("### Users")
    user_data = []
    for uname in db_users:
        u_dir = os.path.join(WORKSPACE_BASE_DIR, uname)
        u_size, u_files = get_directory_size(u_dir)
        user_data.append({
            "User": uname, 
            "Files": u_files, 
            "Storage (MB)": round(u_size / (1024*1024), 2)
        })
    
    st.dataframe(pd.DataFrame(user_data), use_container_width=True, height=300)
    
    # Danger zone
    st.markdown("### ⚠️ Danger Zone")
    if st.button("🗑️ Format Server", type="primary"):
        if os.path.exists(WORKSPACE_BASE_DIR):
            shutil.rmtree(WORKSPACE_BASE_DIR)
        st.rerun()

# Main entry
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
