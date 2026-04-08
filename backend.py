import os
import json
import pandas as pd
import streamlit as st
import tempfile
from fpdf import FPDF
from datetime import datetime

WORKSPACE_BASE_DIR = "workspaces"

def init_workspace(username):
    """Create isolated workspace directory for user"""
    user_dir = os.path.join(WORKSPACE_BASE_DIR, username)
    os.makedirs(user_dir, exist_ok=True)
    return user_dir

def load_user_workspace(username):
    """Load all saved files from user's workspace into session state"""
    user_dir = init_workspace(username)
    if "files" not in st.session_state:
        st.session_state.files = {}
        
    for fname in os.listdir(user_dir):
        if fname.endswith(('.csv', '.xlsx')):
            filepath = os.path.join(user_dir, fname)
            if fname not in st.session_state.files:
                try:
                    df = pd.read_csv(filepath) if fname.endswith('.csv') else pd.read_excel(filepath)
                    st.session_state.files[fname] = {
                        "df": df, 
                        "source": "Saved Workspace File",
                        "is_shared": False
                    }
                except Exception as e:
                    pass

def save_chat_history(username, messages):
    """Save user chat history"""
    user_dir = init_workspace(username)
    history_file = os.path.join(user_dir, "chat_history.json")
    with open(history_file, 'w') as f:
        json.dump(messages, f)

def get_directory_size(path):
    """Calculate total size of a directory"""
    total = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += get_directory_size(entry.path)
    return total

def generate_pdf_report(df, dataset_name, username):
    """Generate a clean Executive PDF Report safely"""
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", 'B', 22)
    pdf.set_text_color(121, 40, 202)
    pdf.cell(0, 15, "DataForge Studio - Executive Report", 0, 1, 'C')
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
    pdf.cell(0, 10, "Generated automatically by the AI Engine", 0, 0, 'C')
    
    # BULLETPROOF PDF EXPORT
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        pdf.output(tmp.name)
        tmp_path = tmp.name
        
    with open(tmp_path, "rb") as f:
        pdf_bytes = f.read()
        
    os.remove(tmp_path)
    return pdf_bytes
