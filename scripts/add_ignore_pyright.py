import json
import sys

"""Usage:

poetry run pyright owid tests --outputjson | python ../../scripts/add_ignore_pyright.py
"""


def add_type_ignore_to_lines(file_path, line_number, rule):
    """Reads a file, adds # type: ignore to the specified line, and writes it back."""
    with open(file_path, "r") as file:
        lines = file.readlines()

    # We need to insert # type: ignore[rule] at the end of the line
    target_line = lines[line_number - 1].rstrip()  # Pyright uses 1-based indexing for lines
    if "# type: ignore" not in target_line:
        lines[line_number - 1] = f"{target_line}  # type: ignore[{rule}]\n"

    with open(file_path, "w") as file:
        file.writelines(lines)


def process_pyright_json_from_pipe():
    """Reads JSON input from stdin (pipe) and adds # type: ignore for errors."""
    data = json.load(sys.stdin)  # Reading from stdin

    for diagnostic in data.get("generalDiagnostics", []):
        file_path = diagnostic["file"]
        line_number = diagnostic["range"]["start"]["line"] + 1  # Pyright uses 0-based index
        rule = diagnostic["rule"]
        print(f"Adding # type: ignore[{rule}] to {file_path} on line {line_number}")

        # Add the # type: ignore comment to the corresponding file and line
        add_type_ignore_to_lines(file_path, line_number, rule)


# Entry point for reading from stdin
if __name__ == "__main__":
    process_pyright_json_from_pipe()
