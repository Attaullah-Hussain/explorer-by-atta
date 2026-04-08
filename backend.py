import os
import json
import pandas as pd
import streamlit as st
from fpdf import FPDF
import hashlib
import functools
from typing import Dict, Optional
import threading
import queue

WORKSPACE_BASE_DIR = "workspaces"

# Thread-safe cache with TTL
class DataFrameCache:
    """LRU Cache for DataFrames with TTL"""
    def __init__(self, maxsize=100, ttl=300):
        self.cache = {}
        self.timestamps = {}
        self.lock = threading.Lock()
        self.maxsize = maxsize
        self.ttl = ttl
    
    def get(self, key):
        with self.lock:
            if key in self.cache:
                if pd.Timestamp.now().timestamp() - self.timestamps[key] < self.ttl:
                    return self.cache[key]
                else:
                    del self.cache[key]
                    del self.timestamps[key]
            return None
    
    def set(self, key, value):
        with self.lock:
            if len(self.cache) >= self.maxsize:
                # Remove oldest
                oldest = min(self.timestamps, key=self.timestamps.get)
                del self.cache[oldest]
                del self.timestamps[oldest]
            
            self.cache[key] = value
            self.timestamps[key] = pd.Timestamp.now().timestamp()
    
    def clear_user(self, username):
        with self.lock:
            keys_to_remove = [k for k in self.cache if k.startswith(f"{username}:")]
            for k in keys_to_remove:
                del self.cache[k]
                del self.timestamps[k]

# Global cache instance
_df_cache = DataFrameCache(maxsize=200, ttl=300)

def cache_dataframe(username, filename, df):
    """Cache dataframe in memory"""
    key = f"{username}:{hashlib.md5(filename.encode()).hexdigest()}"
    _df_cache.set(key, df)

def get_cached_dataframe(username, filename):
    """Retrieve cached dataframe"""
    key = f"{username}:{hashlib.md5(filename.encode()).hexdigest()}"
    return _df_cache.get(key)

def clear_user_cache(username):
    """Clear all cached data for user"""
    _df_cache.clear_user(username)

def init_workspace(username):
    """Initialize user workspace directories"""
    user_dir = os.path.join(WORKSPACE_BASE_DIR, username, "data")
    os.makedirs(user_dir, exist_ok=True)
    return user_dir

def load_user_workspace(username):
    """Load user workspace with caching"""
    user_dir = os.path.join(WORKSPACE_BASE_DIR, username, "data")
    chat_file = os.path.join(WORKSPACE_BASE_DIR, username, "chat_history.json")
    
    files = {}
    
    if os.path.exists(user_dir):
        for filename in os.listdir(user_dir):
            file_path = os.path.join(user_dir, filename)
            ext = filename.split('.')[-1].lower()
            
            if ext not in ["csv", "xlsx", "xls"]:
                continue
            
            try:
                # Try cache first
                cached = get_cached_dataframe(username, filename)
                if cached is not None:
                    files[filename] = {"df": cached}
                    continue
                
                # Load from disk and cache
                if ext == "csv":
                    df = pd.read_csv(file_path)
                else:
                    df = pd.read_excel(file_path)
                
                cache_dataframe(username, filename, df)
                files[filename] = {"df": df}
                
            except Exception as e:
                print(f"Failed to load {filename}: {e}")
    
    st.session_state.files = files
    st.session_state.chat_history_db = {}
    
    if os.path.exists(chat_file):
        try:
            with open(chat_file, "r") as f:
                st.session_state.chat_history_db = json.load(f)
        except:
            pass

def save_chat_history(username):
    """Save chat history atomically"""
    chat_file = os.path.join(WORKSPACE_BASE_DIR, username, "chat_history.json")
    temp_file = chat_file + ".tmp"
    
    try:
        with open(temp_file, "w") as f:
            json.dump(st.session_state.chat_history_db, f)
        os.replace(temp_file, chat_file)
    except Exception as e:
        print(f"Failed to save chat: {e}")
        if os.path.exists(temp_file):
            os.remove(temp_file)

def get_directory_size(path):
    """Calculate directory size efficiently"""
    total_size = 0
    total_files = 0
    
    if not os.path.exists(path):
        return 0, 0
    
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                try:
                    total_size += os.path.getsize(fp)
                    total_files += 1
                except:
                    pass
    
    return total_size, total_files

def generate_pdf_report(df, dataset_name, username):
    """Generate PDF report with error handling"""
    try:
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", 'B', 22)
        pdf.set_text_color(121, 40, 202)
        pdf.cell(0, 15, "Explorer by Atta - Report", 0, 1, 'C')
        pdf.set_font("Arial", 'I', 12)
        pdf.set_text_color(100, 100, 100)
        pdf.cell(0, 10, f"User: {username} | Dataset: {dataset_name}", 0, 1, 'C')
        pdf.line(10, 35, 200, 35)
        pdf.ln(10)
        
        pdf.set_font("Arial", 'B', 16)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 10, "Dataset Overview", 0, 1)
        pdf.set_font("Arial", '', 12)
        pdf.cell(0, 8, f"Rows: {len(df):,}", 0, 1)
        pdf.cell(0, 8, f"Columns: {len(df.columns)}", 0, 1)
        pdf.cell(0, 8, f"Missing Values: {df.isna().sum().sum():,}", 0, 1)
        
        return pdf.output(dest='S').encode('latin-1')
    except Exception as e:
        print(f"PDF generation failed: {e}")
        return b""

class AsyncProcessor:
    """Background task processor for heavy operations"""
    def __init__(self, max_workers=5):
        self.queue = queue.Queue()
        self.results = {}
        self.lock = threading.Lock()
        self.workers = []
        
        for _ in range(max_workers):
            t = threading.Thread(target=self._worker, daemon=True)
            t.start()
            self.workers.append(t)
    
    def _worker(self):
        while True:
            task_id, func, args, kwargs = self.queue.get()
            try:
                result = func(*args, **kwargs)
                with self.lock:
                    self.results[task_id] = ("success", result)
            except Exception as e:
                with self.lock:
                    self.results[task_id] = ("error", str(e))
            finally:
                self.queue.task_done()
    
    def submit(self, func, *args, **kwargs):
        task_id = hashlib.md5(
            f"{func.__name__}:{args}:{kwargs}:{pd.Timestamp.now()}".encode()
        ).hexdigest()
        
        self.queue.put((task_id, func, args, kwargs))
        return task_id
    
    def get_result(self, task_id, timeout=None):
        start = pd.Timestamp.now().timestamp()
        while True:
            with self.lock:
                if task_id in self.results:
                    return self.results.pop(task_id)
            
            if timeout and (pd.Timestamp.now().timestamp() - start) > timeout:
                return ("timeout", None)
            
            threading.Event().wait(0.1)

# Global async processor
async_processor = AsyncProcessor(max_workers=10)
