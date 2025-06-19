#!/usr/bin/env python3

# Test the string detection logic with actual content from the migration file

def isInPythonString(text, position):
    beforeText = text[:position]
    stringRegex_pattern = r'r?(\'\'\'|"""|\'|")'
    
    import re
    matches = list(re.finditer(stringRegex_pattern, beforeText))
    
    inString = False
    stringDelimiter = ''
    
    for match in matches:
        delimiter = match.group(1)
        
        if not inString:
            inString = True
            stringDelimiter = delimiter
        elif delimiter == stringDelimiter:
            inString = False
            stringDelimiter = ''
    
    return inString

# Test with the actual content from the migration file
test_line = '        "description": "The total number of [immigrants](#dod:immigrant) (people moving into a given country) minus the number of [emigrants](#dod:emigrant) (people moving out of the country).",'

# Find DOD references
import re
dod_regex = r'\[([^\]]+)\]\(#dod:([^)]+)\)'
matches = list(re.finditer(dod_regex, test_line))

print(f"Line: {test_line}")
print(f"Found {len(matches)} DOD references:")

for i, match in enumerate(matches):
    start_pos = match.start()
    title = match.group(1)
    key = match.group(2)
    is_in_string = isInPythonString(test_line, start_pos)
    
    print(f"  {i+1}. [{title}](#dod:{key}) at position {start_pos}")
    print(f"     In string: {is_in_string}")
    print(f"     Text before: '{test_line[:start_pos]}'")
    
    # Check what delimiters we find
    beforeText = test_line[:start_pos]
    stringRegex_pattern = r'r?(\'\'\'|"""|\'|")'
    delimiter_matches = list(re.finditer(stringRegex_pattern, beforeText))
    print(f"     String delimiters found: {[m.group(1) for m in delimiter_matches]}")
    print()

print("Expected: Both DOD references should be detected as being inside strings")