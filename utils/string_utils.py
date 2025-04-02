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

def remove_placeholders(str1, str2, ignored_patterns=["cscsusername", "cscsaccount"]):
    """
    Removes one of the given placeholders if it appears in exactly one of the strings.
    The corresponding portion in the other string (up to the next '/', '\n', or space) is also removed.

    :param str1: First string.
    :param str2: Second string.
    :param ignored_patterns: List of placeholders to ignore if they appear in exactly one of the strings.
    :return: Modified versions of str1 and str2.
    """
    
    for ignored_pattern in ignored_patterns:
        # Check if one string contains the placeholder but the other does not
        if ignored_pattern in str1 and ignored_pattern not in str2:
            username_str, other_str = str1, str2
        elif ignored_pattern in str2 and ignored_pattern not in str1:
            username_str, other_str = str2, str1
        else:
            continue  # If neither or both contain it, move to the next placeholder

        # Find the position of the placeholder in the string that contains it
        pos = username_str.find(ignored_pattern)
        if pos == -1:
            continue  # Should never happen, but just in case

        # Remove the placeholder from the string that contains it
        modified_username_str = username_str[:pos] + username_str[pos+len(ignored_pattern):]

        # Remove the corresponding part from the other string up to the next '/', '\n', ' ' or end
        match = re.search(r'[/\n ]', other_str[pos:])  # Find next '/' or '\n' after `pos`
        if match:
            end_pos = pos + match.start()  # Absolute position in the string
            modified_other_str = other_str[:pos] + other_str[end_pos:]
        else:
            modified_other_str = other_str[:pos]  # No match found → Remove everything till end

        # Update str1 and str2 for further processing
        str1, str2 = modified_username_str, modified_other_str
    return str1, str2
    
def normalize_text(text):
    """
    - Removes extra empty lines.
    - Ensures a maximum of one space between words (but keeps line breaks).
    - Removes everything after '#SBATCH --account=' up to the newline.
    """
    if text is None:
        return ""  # Treat None as empty string

    # Normalize each line: strip leading/trailing spaces and reduce multiple spaces to one
    lines = [re.sub(r"\s+", " ", line.strip()) for line in text.splitlines() if line.strip()]

    # Replace '#SBATCH --account=' followed by anything with just '#SBATCH --account='
    #lines = [re.sub(r"(#SBATCH --account=).*", r"\1", line) for line in lines]

    # Join lines back together while preserving newlines
    return "\n".join(lines)