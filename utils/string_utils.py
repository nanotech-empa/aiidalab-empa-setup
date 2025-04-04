from datetime import datetime
import re
# labels to rename old host and codes
def extract_first_column(command_output):
    """Extracts only the first column (UENV image names) from multi-column output."""
    lines = command_output.split("\n")[1:]  # Skip the header line
    return {line.split()[0] for line in lines if line.strip()}  # Get first column values

def relabel(label):
    """Assumes that in case @ is present it is a code and keeps only the portion preceding @ """
    before_at, at, after_at = label.partition('@')
    return datetime.now().strftime("%Y%m%d%H%M")+'_'+before_at

def to_camel_case(snake_str):
    """
    Converts a snake_case string to CamelCase (PascalCase).
    
    Example:
        "string" -> "String"
        "string_second" -> "StringSecond"
        "another_nice_string" -> "AnotherNiceString"
    """
    return ''.join(word.capitalize() for word in snake_str.split('_'))

def remove_green_check_lines(html_string):
    """
    Removes lines containing the green checkbox (✅) from an HTML-formatted string with <br> separators.

    :param html_string: A string containing HTML with <br> as line separators.
    :return: A modified HTML string with ✅ lines removed.
    """
    # Split by <br> instead of \n
    lines = html_string.split("<br>")
    
    # Keep only lines that do NOT contain '✅'
    filtered_lines = [line for line in lines if "✅" not in line]
    
    # Rejoin the lines back with <br> to maintain HTML format
    return "<br>".join(filtered_lines)
    
def normalize_text(text):
    """
    - Removes extra empty lines.
    - Ensures a maximum of one space between words (but keeps line breaks).
    """
    if text is None:
        return ""  # Treat None as empty string

    # Normalize each line: strip leading/trailing spaces and reduce multiple spaces to one
    lines = [re.sub(r"\s+", " ", line.strip()) for line in text.splitlines() if line.strip()]

    # Join lines back together while preserving newlines
    return "\n".join(lines)