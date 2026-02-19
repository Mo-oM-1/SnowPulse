"""
SnowPulse - Shared Snowflake connection module
Used by all Streamlit pages
Supports both local (file path) and cloud (inline key) auth
"""

import streamlit as st
import pandas as pd
from snowflake.connector import connect
from cryptography.hazmat.primitives import serialization
from pathlib import Path


@st.cache_resource
def get_connection():
    """Create a cached Snowflake connection using RSA key-pair auth."""
    sf = st.secrets["snowflake"]

    # Cloud mode: private key content stored directly in secrets
    if "private_key" in sf:
        key_bytes = sf["private_key"].encode("utf-8")
    # Local mode: private key as a file path
    else:
        key_path = Path(sf["private_key_path"])
        with open(key_path, "rb") as f:
            key_bytes = f.read()

    p_key = serialization.load_pem_private_key(key_bytes, password=None)
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return connect(
        account=sf["account"],
        user=sf["user"],
        private_key=pkb,
        role=sf["role"],
        warehouse=sf["warehouse"],
        database=sf["database"],
    )


@st.cache_data(ttl=60)
def run_query(sql: str) -> pd.DataFrame:
    """Execute a SQL query and return results as a DataFrame. Cached for 60s."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)
