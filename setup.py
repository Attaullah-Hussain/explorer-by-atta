import os
import subprocess
import sys

def setup_app():
    """Setup the environment and run the app"""
    base_dir = r"C:\Users\atakh\OneDrive\Desktop\smart-file-explorer"
    streamlit_path = r"C:\Users\atakh\AppData\Local\Programs\Python\Python312\Scripts\streamlit.exe"
    
    # Change to project directory
    os.chdir(base_dir)
    print(f"📁 Working in: {base_dir}")
    
    # 1. Create .streamlit folder
    streamlit_dir = os.path.join(base_dir, ".streamlit")
    if not os.path.exists(streamlit_dir):
        os.makedirs(streamlit_dir)
        print("✅ Created .streamlit folder")
    else:
        print("ℹ️ .streamlit folder already exists")
    
    # 2. Create secrets.toml
    secrets_file = os.path.join(streamlit_dir, "secrets.toml")
    secrets_content = '''GROQ_API_KEY = "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"
'''
    with open(secrets_file, "w") as f:
        f.write(secrets_content)
    print("✅ Created secrets.toml with API key")
    
    # 3. Delete old database if exists
    db_file = os.path.join(base_dir, "users.db")
    if os.path.exists(db_file):
        os.remove(db_file)
        print("✅ Deleted old database (users.db)")
    else:
        print("ℹ️ No old database found")
    
    # 4. Create .gitignore if not exists
    gitignore_file = os.path.join(base_dir, ".gitignore")
    if not os.path.exists(gitignore_file):
        gitignore_content = """# Secrets - NEVER commit these!
.streamlit/secrets.toml
.env
__pycache__/
*.pyc
workspaces/
*.db
"""
        with open(gitignore_file, "w") as f:
            f.write(gitignore_content)
        print("✅ Created .gitignore")
    
    print("\n🚀 Setup complete! Starting Streamlit app...\n")
    
    # 5. Run Streamlit app
    app_path = os.path.join(base_dir, "app.py")
    subprocess.Popen([streamlit_path, "run", app_path], 
                     creationflags=subprocess.CREATE_NEW_CONSOLE)
    
    print("✅ Streamlit app started in new window!")
    print("➡️  Check your browser at: http://localhost:8501")

if __name__ == "__main__":
    setup_app()