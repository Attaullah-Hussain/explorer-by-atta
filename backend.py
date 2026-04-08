import os
import json
import pandas as pd
import streamlit as st
from fpdf import FPDF

WORKSPACE_BASE_DIR = "workspaces"

def init_workspace(username):
    user_dir = os.path.join(WORKSPACE_BASE_DIR, username, "data")
    os.makedirs(user_dir, exist_ok=True)
    return user_dir

def load_user_workspace(username):
    user_dir = os.path.join(WORKSPACE_BASE_DIR, username, "data")
    chat_file = os.path.join(WORKSPACE_BASE_DIR, username, "chat_history.json")
    
    st.session_state.files = {}
    if os.path.exists(user_dir):
        for filename in os.listdir(user_dir):
            file_path = os.path.join(user_dir, filename)
            ext = filename.split('.')[-1].lower()
            try:
                if ext == "csv": df = pd.read_csv(file_path)
                elif ext in ["xlsx", "xls"]: df = pd.read_excel(file_path)
                else: continue
                st.session_state.files[filename] = {"df": df}
            except Exception as e: print(f"Failed to load {filename}: {e}")

    st.session_state.chat_history_db = {}
    if os.path.exists(chat_file):
        try:
            with open(chat_file, "r") as f:
                st.session_state.chat_history_db = json.load(f)
        except: pass

def save_chat_history(username):
    chat_file = os.path.join(WORKSPACE_BASE_DIR, username, "chat_history.json")
    with open(chat_file, "w") as f:
        json.dump(st.session_state.chat_history_db, f)

def get_directory_size(path):
    total_size = 0
    total_files = 0
    if os.path.exists(path):
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
                    total_files += 1
    return total_size, total_files

def generate_pdf_report(df, dataset_name, username):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", 'B', 22)
    pdf.set_text_color(121, 40, 202)
    pdf.cell(0, 15, "Explorer by Atta - Executive Report", 0, 1, 'C')
    pdf.set_font("Arial", 'I', 12)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 10, f"Generated for: {username} | Dataset: {dataset_name}", 0, 1, 'C')
    pdf.line(10, 35, 200, 35)
    pdf.ln(10)
    pdf.set_font("Arial", 'B', 16)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 10, "1. Dataset Overview", 0, 1)
    pdf.set_font("Arial", '', 12)
    pdf.cell(0, 8, f"- Total Rows Processed: {len(df):,}", 0, 1)
    pdf.cell(0, 8, f"- Total Columns Detected: {len(df.columns)}", 0, 1)
    pdf.cell(0, 8, f"- Total Missing Values: {df.isna().sum().sum():,}", 0, 1)
    pdf.ln(5)
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(0, 10, "2. Data Schema & Columns", 0, 1)
    pdf.set_font("Arial", '', 11)
    columns_str = ", ".join(df.columns.tolist())
    pdf.multi_cell(0, 7, f"Available Columns: {columns_str}")
    pdf.ln(5)
    pdf.set_y(-30)
    pdf.set_font("Arial", 'I', 9)
    pdf.set_text_color(150, 150, 150)
    pdf.cell(0, 10, "Generated automatically by the Explorer by Atta AI Engine", 0, 0, 'C')
    return bytes(pdf.output())
