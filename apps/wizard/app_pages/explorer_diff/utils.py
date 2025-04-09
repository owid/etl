import streamlit as st


def truncate_lines(s: str, max_lines: int) -> str:
    """
    Truncate a string to a maximum number of lines.
    """
    lines = s.splitlines()
    if len(lines) > max_lines:
        st.warning(f"The diff is too long to display in full. Showing only the first {max_lines} lines.")
        return "\n".join(lines[:max_lines]) + "\n... (truncated)"
    return s
