import json
import sys

"""Usage:

uv run pyright owid tests --outputjson | python ../../scripts/add_ignore_pyright.py

Note: This script is a pyright-specific helper. It intentionally inserts
# type: ignore[rule] comments (recognised by pyright/mypy) rather than
# ty: ignore[rule] (recognised by ty). Do not use it to suppress ty errors.
"""


def add_type_ignore_to_lines(file_path, line_number, rule):
    """Reads a file, adds # ty: ignore to the specified line, and writes it back."""
    with open(file_path) as file:
        lines = file.readlines()

    # We need to insert # ty: ignore at the end of the line
    target_line = lines[line_number - 1].rstrip()  # Pyright uses 1-based indexing for lines
    if "# ty: ignore" not in target_line and "# ty: ignore" not in target_line:
        lines[line_number - 1] = f"{target_line}  # ty: ignore\n"

    with open(file_path, "w") as file:
        file.writelines(lines)


def process_pyright_json_from_pipe():
    """Reads JSON input from stdin (pipe) and adds # ty: ignore for errors."""
    data = json.load(sys.stdin)  # Reading from stdin

    for diagnostic in data.get("generalDiagnostics", []):
        file_path = diagnostic["file"]
        line_number = diagnostic["range"]["start"]["line"] + 1  # Pyright uses 0-based index
        rule = diagnostic["rule"]
        print(f"Adding # ty: ignore to {file_path} on line {line_number}")

        # Add the # ty: ignore comment to the corresponding file and line
        add_type_ignore_to_lines(file_path, line_number, rule)


# Entry point for reading from stdin
if __name__ == "__main__":
    process_pyright_json_from_pipe()
