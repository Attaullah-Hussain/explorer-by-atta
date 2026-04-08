import streamlit as st

def inject_login_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap');
    .stApp { background-color: #0B0C10 !important; background-image: radial-gradient(circle at 15% 50%, rgba(121, 40, 202, 0.15), transparent 35%), radial-gradient(circle at 85% 30%, rgba(255, 0, 122, 0.15), transparent 35%); font-family: 'Poppins', sans-serif; }
    [data-testid="block-container"] { max-width: 1000px; padding-top: 8vh; }
    [data-testid="column"]:nth-child(2) { background: #151720; border: 1px solid rgba(255, 255, 255, 0.05); border-radius: 20px; padding: 50px 60px !important; box-shadow: 0 25px 50px -12px rgba(0,0,0,0.5); position: relative; overflow: hidden; z-index: 10; }
    [data-testid="column"]:nth-child(2)::before { content: ""; position: absolute; top: 0; left: 0; right: 0; height: 3px; background: linear-gradient(90deg, #FF007A, #7928CA); }
    [data-testid="column"]:nth-child(2) * { color: #E0E6ED !important; }
    .stTextInput > div > div { background-color: transparent !important; border: none !important; box-shadow: none !important; }
    .stTextInput input { background-color: #1A1C26 !important; border: 1px solid rgba(255, 255, 255, 0.05) !important; border-radius: 8px !important; color: #FFFFFF !important; padding: 16px !important; font-size: 0.9rem !important; font-weight: 400; transition: all 0.3s ease; }
    .stTextInput input:focus { background-color: #1F222E !important; border: 1px solid #7928CA !important; box-shadow: 0 0 15px rgba(121, 40, 202, 0.2) !important; }
    .stTextInput input::placeholder { color: #5C6275 !important; }
    div.stButton > button[kind="primary"] { background: linear-gradient(135deg, #FF007A 0%, #7928CA 100%) !important; color: #FFFFFF !important; border: none !important; border-radius: 8px !important; padding: 14px !important; font-weight: 600 !important; font-size: 1rem !important; letter-spacing: 1px; width: 100%; margin-top: 20px; transition: all 0.3s ease; box-shadow: 0 4px 15px rgba(255, 0, 122, 0.3) !important; }
    div.stButton > button[kind="primary"]:hover { transform: translateY(-2px); box-shadow: 0 8px 25px rgba(255, 0, 122, 0.5) !important; }
    div.stButton > button[kind="primary"] * { color: #FFFFFF !important; }
    label[data-testid="stWidgetLabel"] { display: none; }
    .vector-title { font-size: 2.2rem; font-weight: 700; margin-bottom: 5px; background: -webkit-linear-gradient(45deg, #FFFFFF, #A0A5B5); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
    .vector-subtitle { color: #5C6275; font-size: 0.9rem; margin-bottom: 30px; }
    
    /* MOBILE FIX FOR LOGIN */
    @media (max-width: 768px) { 
        [data-testid="column"]:nth-child(1), [data-testid="column"]:nth-child(3) { display: none !important; } 
        [data-testid="column"]:nth-child(2) { padding: 30px 25px !important; width: 100% !important; min-width: 100% !important; } 
        [data-testid="block-container"] { padding-top: 4vh; padding-left: 1rem; padding-right: 1rem; } 
        .vector-title { font-size: 1.8rem; } 
        .vector-subtitle { font-size: 0.8rem; } 
    }
    </style>
    """, unsafe_allow_html=True)

def inject_dashboard_css(theme: str):
    if theme == "Light": 
        bg, card_bg, text, border = "#f8fafc", "#ffffff", "#0f172a", "#e2e8f0"
    else: 
        bg, card_bg, text, border = "#0B0C10", "#151720", "#E0E6ED", "#1F222E"

    st.markdown(f"""
    <style>
    .stApp {{ background-color: {bg} !important; background-image: none !important; color: {text}; }}
    [data-testid="stSidebar"] {{ background-color: {card_bg} !important; border-right: 1px solid {border}; }}
    .metric-card {{ background-color: {card_bg}; border: 1px solid {border}; border-radius: 12px; padding: 24px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); margin-bottom: 15px; }}
    h1, h2, h3, p, span {{ color: {text} !important; }}
    [data-testid="stChatMessage"] {{ background-color: transparent; border-radius: 10px; padding: 10px; }}
    [data-testid="stChatMessageContent"] {{ color: {text} !important; }}
    </style>
    """, unsafe_allow_html=True)

def inject_mobile_css():
    """Forces Streamlit layout to be completely mobile-friendly for the Dashboard"""
    st.markdown("""
    <style>
    /* MOBILE FIX FOR DASHBOARD */
    @media (max-width: 768px) {
        /* Columns ko ek ke neechay ek (stack) kar do */
        [data-testid="column"] {
            width: 100% !important;
            flex: 1 1 100% !important;
            min-width: 100% !important;
            margin-bottom: 15px !important;
        }
        
        /* Dataframes ko horizontal scroll karne do */
        [data-testid="stDataFrame"] {
            width: 100% !important;
            overflow-x: auto !important;
        }
        
        /* Text aur Cards ko mobile ke liye set karo */
        .metric-card { padding: 15px; text-align: center; } 
        h2 { font-size: 1.5rem !important; } 
        [data-testid="stSidebar"] { padding-top: 2rem !important; }
        
        [data-testid="stMetricValue"] {
            font-size: 1.6rem !important;
        }
        
        /* Chart container ki width 100% set karo */
        .js-plotly-plot {
            max-width: 100% !important;
        }
    }
    </style>
    """, unsafe_allow_html=True)
