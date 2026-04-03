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

# ---> NEW: Importing our SQL Database modules <---
from auth import init_db, add_user, get_user_hash, get_all_users, check_password
from styles import inject_login_css, inject_dashboard_css
from backend import init_workspace, load_user_workspace, save_chat_history, get_directory_size, generate_pdf_report

GROQ_API_KEY = st.secrets["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"]
WORKSPACE_BASE_DIR = "workspaces"

# Initialize the SQL Database on app startup
init_db()

st.set_page_config(page_title="Explorer by Atta", page_icon="✨", layout="wide", initial_sidebar_state="auto")

if "logged_in" not in st.session_state: st.session_state.logged_in = False
if "theme" not in st.session_state: st.session_state.theme = "Light"

def show_login():
    inject_login_css()
    _, col_center, _ = st.columns([1, 1.2, 1])
    
    with col_center:
        st.markdown("""
        <div style="margin-bottom: 20px;">
            <div style="width: 40px; height: 40px; border-radius: 10px; background: linear-gradient(135deg, #FF007A, #7928CA); margin-bottom: 15px; display: flex; align-items: center; justify-content: center;">
                <span style="color: white; font-weight: bold; font-size: 1.2rem;">✨</span>
            </div>
            <div class='vector-title'>Explorer by Atta</div>
            <div class='vector-subtitle'>Sign in to continue to your intelligent workspace</div>
        </div>
        """, unsafe_allow_html=True)
        
        u = st.text_input("Username", placeholder="Username", key="login_user")
        p = st.text_input("Password", type="password", placeholder="Password", key="login_pass")
        
        st.markdown("""
        <div style='display: flex; justify-content: space-between; align-items: center; margin-top: 10px;'>
            <label style='color: #5C6275; font-size: 0.8rem; display: flex; align-items: center; gap: 5px;'><input type="checkbox" checked style="accent-color: #7928CA;"> Remember me</label>
            <a href='#' style='color: #FF007A; font-size: 0.8rem; text-decoration: none; font-weight: 500;'>Forgot password?</a>
        </div>
        """, unsafe_allow_html=True)

        def login_success(username):
            st.session_state.logged_in = True
            st.session_state.user = username
            init_workspace(username)
            load_user_workspace(username)
            st.rerun()

        if st.button("Sign In / Register", type="primary", use_container_width=True):
            if not u.strip() or not p.strip():
                st.warning("⚠️ Please enter both a username and a password.")
            else:
                # ---> NEW: Querying the SQL Database <---
                hashed_pw = get_user_hash(u)
                
                if hashed_pw is None: # User does not exist, register them
                    add_user(u, p)
                    st.success("Account created in SQL Database! Preparing workspace...")
                    login_success(u)
                elif check_password(p, hashed_pw): # User exists, verify password
                    login_success(u)
                else: 
                    st.error("Invalid credentials.")

def show_dashboard():
    if "files" not in st.session_state: load_user_workspace(st.session_state.user)
    
    inject_dashboard_css(st.session_state.theme)
    user_dir = init_workspace(st.session_state.user)
    
    with st.sidebar:
        st.markdown("### ✨ Explorer by Atta")
        st.markdown(f"**👤 {st.session_state.user}**")
        
        if st.session_state.user == "Admin": st.caption("Admin God Mode Active 👑")
        else: st.caption("Secure Workspace Active 🔒")
        
        theme_choice = st.radio("Theme", ["Light", "Dark"], index=1 if st.session_state.theme == "Dark" else 0, horizontal=True)
        if theme_choice != st.session_state.theme:
            st.session_state.theme = theme_choice
            st.rerun()
            
        if st.button("Sign Out", use_container_width=True):
            for key in list(st.session_state.keys()): del st.session_state[key]
            st.rerun()
        
        st.divider()
        nav_options = ["Overview", "Live Data API 🌐", "Data Cleaning", "Visual Explorer", "ML Forecasting", "AI Chat Assistant"]
        if st.session_state.user == "Admin": nav_options.append("Admin Console 👑")
            
        page = st.radio("Navigation", nav_options)
        
        st.divider()
        if page != "Admin Console 👑":
            st.markdown("**Cloud Storage**")
            up = st.file_uploader("Upload to Workspace", type=["csv", "xlsx", "zip"], accept_multiple_files=True, label_visibility="collapsed")
            
            if up:
                for f in up:
                    ext = f.name.split('.')[-1].lower()
                    if ext == 'zip':
                        with zipfile.ZipFile(f, 'r') as zip_ref:
                            extracted_count = 0
                            for zip_info in zip_ref.infolist():
                                if zip_info.is_dir() or zip_info.filename.startswith('__MACOSX/') or zip_info.filename.split('/')[-1].startswith('.'): continue
                                inner_ext = zip_info.filename.split('.')[-1].lower()
                                if inner_ext in ['csv', 'xlsx', 'xls']:
                                    safe_filename = os.path.basename(zip_info.filename)
                                    if safe_filename not in st.session_state.files:
                                        save_path = os.path.join(user_dir, safe_filename)
                                        with open(save_path, "wb") as out_file: out_file.write(zip_ref.read(zip_info.filename))
                                        try:
                                            df = pd.read_csv(save_path) if inner_ext == 'csv' else pd.read_excel(save_path)
                                            st.session_state.files[safe_filename] = {"df": df}
                                            extracted_count += 1
                                        except Exception as e: st.error(f"Failed to read {safe_filename}: {e}")
                            if extracted_count > 0: st.success(f"Extracted {extracted_count} files!")
                    else:
                        if f.name not in st.session_state.files:
                            save_path = os.path.join(user_dir, f.name)
                            with open(save_path, "wb") as out_file: out_file.write(f.getbuffer())
                            df = pd.read_csv(f) if ext == 'csv' else pd.read_excel(f)
                            st.session_state.files[f.name] = {"df": df}
                            st.success(f"Saved {f.name}!")

            if st.session_state.files:
                if st.button("🗑️ Clear Workspace Data", use_container_width=True):
                    shutil.rmtree(user_dir)
                    chat_file = os.path.join(WORKSPACE_BASE_DIR, st.session_state.user, "chat_history.json")
                    if os.path.exists(chat_file): os.remove(chat_file)
                    init_workspace(st.session_state.user)
                    load_user_workspace(st.session_state.user)
                    st.rerun()

    if page == "Admin Console 👑":
        st.markdown("<h2>👑 Admin Control Panel</h2>", unsafe_allow_html=True)
        # ---> NEW: Fetching User List from SQL Database <---
        db_users = get_all_users()
        total_size_bytes, total_files = get_directory_size(WORKSPACE_BASE_DIR)
        
        c1, c2, c3 = st.columns(3)
        with c1: st.markdown(f"<div class='metric-card'><h4>{len(db_users)}</h4><p>Users in SQL DB</p></div>", unsafe_allow_html=True)
        with c2: st.markdown(f"<div class='metric-card'><h4>{total_files}</h4><p>Files on Server</p></div>", unsafe_allow_html=True)
        with c3: st.markdown(f"<div class='metric-card'><h4>{total_size_bytes / (1024 * 1024):.2f} MB</h4><p>Storage</p></div>", unsafe_allow_html=True)
        
        st.markdown("### 📋 Database Users")
        user_list = []
        for uname in db_users:
            u_dir = os.path.join(WORKSPACE_BASE_DIR, uname)
            u_size, u_files = get_directory_size(u_dir)
            user_list.append({"Username": uname, "Files Saved": u_files, "Storage Used (MB)": round(u_size / (1024 * 1024), 2)})
        st.dataframe(pd.DataFrame(user_list), use_container_width=True)
        
        st.markdown("### ⚠️ Danger Zone")
        if st.button("🗑️ Format Server", type="primary"):
            if os.path.exists(WORKSPACE_BASE_DIR): shutil.rmtree(WORKSPACE_BASE_DIR)
            st.rerun()
        return

    if page == "Live Data API 🌐":
        st.markdown("<h2>🌐 Live Data Extractor</h2>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns(3)
        with col1: ticker = st.text_input("Ticker Symbol", "AAPL")
        with col2: start_date = st.date_input("Start Date", datetime.today() - timedelta(days=365))
        with col3: end_date = st.date_input("End Date", datetime.today())
        
        if st.button("📡 Fetch Live Data", type="primary"):
            try:
                stock_data = yf.Ticker(ticker).history(start=start_date, end=end_date)
                if not stock_data.empty:
                    stock_data.reset_index(inplace=True)
                    if 'Date' in stock_data.columns: stock_data['Date'] = stock_data['Date'].dt.tz_localize(None)
                    safe_filename = f"{ticker}_Live_Data.csv"
                    save_path = os.path.join(user_dir, safe_filename)
                    stock_data.to_csv(save_path, index=False)
                    st.session_state.files[safe_filename] = {"df": stock_data}
                    st.success("Data fetched!")
                    st.plotly_chart(px.line(stock_data, x="Date", y="Close"), use_container_width=True)
            except Exception as e: st.error(f"Error: {e}")
        if not st.session_state.files: return

    if not st.session_state.files:
        st.markdown("<h3 style='text-align:center; opacity:0.5;'>Workspace empty. Upload a file or fetch Live Data.</h3>", unsafe_allow_html=True)
        return

    if page != "Live Data API 🌐":
        selected = st.selectbox("Active Dataset", list(st.session_state.files.keys()))
        df = st.session_state.files[selected]["df"]

    if page == "Overview":
        col_title, col_btn = st.columns([3, 1])
        with col_title: st.markdown("<h2>Data Overview</h2>", unsafe_allow_html=True)
        with col_btn:
            st.download_button("📥 Download PDF", data=generate_pdf_report(df, selected, st.session_state.user), file_name=f"{selected}_Report.pdf", mime="application/pdf", use_container_width=True)

        c1, c2, c3 = st.columns(3)
        with c1: st.markdown(f"<div class='metric-card'><h4>{len(df):,}</h4><p>Rows</p></div>", unsafe_allow_html=True)
        with c2: st.markdown(f"<div class='metric-card'><h4>{len(df.columns)}</h4><p>Columns</p></div>", unsafe_allow_html=True)
        with c3: st.markdown(f"<div class='metric-card'><h4>{df.isna().sum().sum():,}</h4><p>Missing</p></div>", unsafe_allow_html=True)
        st.dataframe(df.head(50), use_container_width=True)

    elif page == "Data Cleaning":
        st.markdown("<h2>🧹 Data Cleaning Studio</h2>", unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("Drop Duplicates", use_container_width=True):
                df.drop_duplicates(inplace=True)
                st.session_state.files[selected]["df"] = df
                df.to_csv(os.path.join(user_dir, selected), index=False) if selected.endswith('.csv') else df.to_excel(os.path.join(user_dir, selected), index=False)
                st.rerun()
        with c2:
            if st.button("Fill Missing with 0", use_container_width=True):
                df.fillna(0, inplace=True)
                st.session_state.files[selected]["df"] = df
                df.to_csv(os.path.join(user_dir, selected), index=False) if selected.endswith('.csv') else df.to_excel(os.path.join(user_dir, selected), index=False)
                st.rerun()
        with c3:
            col_to_rename = st.selectbox("Column", df.columns, label_visibility="collapsed")
            new_col = st.text_input("New Name")
            if st.button("Rename") and new_col:
                df.rename(columns={col_to_rename: new_col}, inplace=True)
                st.session_state.files[selected]["df"] = df
                df.to_csv(os.path.join(user_dir, selected), index=False) if selected.endswith('.csv') else df.to_excel(os.path.join(user_dir, selected), index=False)
                st.rerun()
        st.dataframe(df.head(15), use_container_width=True)

    elif page == "Visual Explorer":
        st.markdown("<h2>📊 Visual Explorer</h2>", unsafe_allow_html=True)
        c1, c2, c3 = st.columns([1,2,2])
        num_cols = df.select_dtypes(include='number').columns
        with c1: chart = st.selectbox("Chart Type", ["Bar", "Scatter", "Line", "Histogram"])
        with c2: x_ax = st.selectbox("X-Axis", df.columns)
        with c3: y_ax = st.selectbox("Y-Axis", num_cols) if chart != "Histogram" else None

        if chart == "Bar": fig = px.bar(df, x=x_ax, y=y_ax)
        elif chart == "Scatter": fig = px.scatter(df, x=x_ax, y=y_ax)
        elif chart == "Line": fig = px.line(df, x=x_ax, y=y_ax)
        else: fig = px.histogram(df, x=x_ax)
        
        fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color="#E0E6ED" if st.session_state.theme == "Dark" else "#0f172a")
        st.plotly_chart(fig, use_container_width=True)

    elif page == "ML Forecasting":
        st.markdown("<h2>🔮 Machine Learning Forecasting</h2>", unsafe_allow_html=True)
        num_cols = df.select_dtypes(include='number').columns.tolist()
        if len(num_cols) == 0: st.warning("No numeric columns.")
        else:
            fc1, fc2, fc3 = st.columns(3)
            with fc1: date_col = st.selectbox("X-Axis (Time)", df.columns)
            with fc2: target_col = st.selectbox("Y-Axis (Target)", num_cols)
            with fc3: steps = st.slider("Periods", 1, 60, 14)

            if st.button("🚀 Run Forecast", type="primary"):
                df_ml = df.copy()
                try:
                    df_ml['ML_DATE'] = pd.to_datetime(df_ml[date_col])
                    df_ml = df_ml.sort_values('ML_DATE')
                    X = df_ml['ML_DATE'].map(pd.Timestamp.toordinal).values.reshape(-1, 1)
                    is_date = True
                except:
                    X = np.arange(len(df_ml)).reshape(-1, 1)
                    is_date = False

                y = df_ml[target_col].values
                model = LinearRegression().fit(X, y)
                past_pred = model.predict(X)
                future_X = np.array([[X[-1][0] + i] for i in range(1, steps + 1)])
                future_pred = model.predict(future_X)
                
                x_past = df_ml['ML_DATE'] if is_date else df_ml[date_col]
                x_future = [df_ml['ML_DATE'].iloc[-1] + timedelta(days=i) for i in range(1, steps + 1)] if is_date else [f"+{i}" for i in range(1, steps + 1)]

                fig = go.Figure()
                fig.add_trace(go.Scatter(x=x_past, y=y, name='Actual', line=dict(color='#7928CA')))
                fig.add_trace(go.Scatter(x=x_past, y=past_pred, name='Trend', line=dict(color='rgba(255,0,122,0.5)', dash='dot')))
                fig.add_trace(go.Scatter(x=x_future, y=future_pred, name='Prediction', line=dict(color='#FF007A')))
                fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)

    elif page == "AI Chat Assistant":
        st.markdown("<h2>🧠 Chat with your Data</h2>", unsafe_allow_html=True)
        chat_key = f"chat_{selected}"
        if chat_key not in st.session_state.chat_history_db:
            st.session_state.chat_history_db[chat_key] = [{"role": "assistant", "content": "Hi! What would you like to know?"}]

        for msg in st.session_state.chat_history_db[chat_key]:
            with st.chat_message(msg["role"]): st.markdown(msg["content"])

        if prompt := st.chat_input("Ask something..."):
            st.session_state.chat_history_db[chat_key].append({"role": "user", "content": prompt})
            with st.chat_message("user"): st.markdown(prompt)

            with st.chat_message("assistant"):
                try:
                    sys_ctx = f"Data Summary: {len(df)} rows. Stats: {df.describe().to_markdown()}"
                    client = OpenAI(api_key=GROQ_API_KEY, base_url="https://api.groq.com/openai/v1")
                    msgs = [{"role": "system", "content": sys_ctx}] + st.session_state.chat_history_db[chat_key][-6:]
                    resp = client.chat.completions.create(model="llama-3.3-70b-versatile", messages=msgs)
                    reply = resp.choices[0].message.content
                    st.markdown(reply)
                    st.session_state.chat_history_db[chat_key].append({"role": "assistant", "content": reply})
                    save_chat_history(st.session_state.user)
                except Exception as e: st.error(f"Error: {e}")

if not st.session_state.logged_in: show_login()
else: show_dashboard()

if __name__ == "__main__":
    import sys
    from streamlit.web import cli as stcli
    if not st.runtime.exists():
        sys.argv = ["streamlit", "run", sys.argv[0]]
        sys.exit(stcli.main())