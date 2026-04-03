import sqlite3
import hashlib
import secrets
import threading
from contextlib import contextmanager

# Thread-local storage for connections
_local = threading.local()

def get_db_connection():
    """Get thread-safe database connection"""
    if not hasattr(_local, 'connection'):
        _local.connection = sqlite3.connect('users.db', check_same_thread=False)
        _local.connection.row_factory = sqlite3.Row
    return _local.connection

@contextmanager
def get_db_cursor():
    """Context manager for database operations"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()

def init_db():
    """Initialize database with WAL mode for better concurrency"""
    conn = sqlite3.connect('users.db', check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL')  # Write-Ahead Logging for concurrency
    conn.execute('PRAGMA synchronous=NORMAL')
    
    cursor = conn.cursor()
    
    # Check if table exists and has correct schema
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
    table_exists = cursor.fetchone()
    
    if table_exists:
        # Check if password_hash column exists
        cursor.execute("PRAGMA table_info(users)")
        columns = [column[1] for column in cursor.fetchall()]
        
        # If using old schema with 'password' column instead of 'password_hash'
        if 'password' in columns and 'password_hash' not in columns:
            # Migrate old table
            cursor.execute("ALTER TABLE users RENAME TO users_old")
            cursor.execute('''
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                INSERT INTO users (id, username, password_hash, created_at)
                SELECT id, username, password, created_at FROM users_old
            ''')
            cursor.execute("DROP TABLE users_old")
            conn.commit()
            print("Database migrated to new schema")
        elif 'password_hash' not in columns:
            # Add password_hash column if missing
            cursor.execute("ALTER TABLE users ADD COLUMN password_hash TEXT")
            conn.commit()
    else:
        # Create new table with correct schema
        cursor.execute('''
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
    
    conn.close()

def hash_password(password):
    """Secure password hashing"""
    salt = secrets.token_hex(16)
    pwdhash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return salt + pwdhash.hex()

def check_password(password, stored_hash):
    """Verify password against stored hash"""
    salt = stored_hash[:32]
    stored_pwdhash = stored_hash[32:]
    pwdhash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return pwdhash.hex() == stored_pwdhash

def add_user(username, password):
    """Add new user with hashed password"""
    with get_db_cursor() as cursor:
        try:
            hashed = hash_password(password)
            cursor.execute(
                "INSERT INTO users (username, password_hash) VALUES (?, ?)",
                (username, hashed)
            )
            return True
        except sqlite3.IntegrityError:
            return False

def get_user_hash(username):
    """Get password hash for username"""
    with get_db_cursor() as cursor:
        cursor.execute("SELECT password_hash FROM users WHERE username = ?", (username,))
        result = cursor.fetchone()
        return result['password_hash'] if result else None

def get_all_users():
    """Get all usernames"""
    with get_db_cursor() as cursor:
        cursor.execute("SELECT username FROM users ORDER BY created_at DESC")
        return [row['username'] for row in cursor.fetchall()]
