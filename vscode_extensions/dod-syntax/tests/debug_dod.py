#!/usr/bin/env python3
"""
Debug script to test DoD pattern matching
"""
import re

# Test the same regex pattern as used in the extension
dod_regex = r'\[([^\]]+)\]\(#dod:([^)]+)\)'

test_content = """
title: "Test DoD Hover"
description: |
  This is a test file to verify DoD syntax highlighting works.
  
  We have a reference to [Cancer](#dod:cancer) which should be highlighted and hoverable.
  
  And another reference to [Diabetes](#dod:diabetes) here.

notes: |
  The pattern should match [title](#dod:key) format.
  Test some more: [Malaria](#dod:malaria)
"""

print("Testing DoD pattern matching...")
print("Regex pattern:", dod_regex)
print("\nTest content:")
print(test_content)
print("\nMatches found:")

matches = re.finditer(dod_regex, test_content)
match_count = 0

for match in matches:
    match_count += 1
    print(f"Match {match_count}: '{match.group(0)}' at position {match.start()}")
    print(f"  Title: '{match.group(1)}'")
    print(f"  Key: '{match.group(2)}'")
    print()

print(f"Total matches found: {match_count}")