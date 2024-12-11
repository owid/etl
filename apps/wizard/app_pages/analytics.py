import streamlit as st

external_url = "http://dashboard.owid.io"

redirect_script = f"""
    <meta http-equiv="refresh" content="0; url={external_url}">
    <script type="text/javascript">
        window.location.href = "{external_url}";
    </script>
    If you are not redirected automatically, follow this <a href='{external_url}'>link</a>.
"""

st.markdown(redirect_script, unsafe_allow_html=True)
