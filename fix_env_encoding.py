#!/usr/bin/env python
# Read file with UTF-16 LE and write back as UTF-8
with open('.env', 'rb') as f:
    raw_data = f.read()

# Try to detect encoding
try:
    # Try UTF-16 LE first (appears to have BOM)
    if raw_data.startswith(b'\xff\xfe'):
        content = raw_data.decode('utf-16-le')
    else:
        content = raw_data.decode('utf-8')
except UnicodeDecodeError as e:
    print(f"Decode error: {e}")
    # Try other encodings
    try:
        content = raw_data.decode('utf-16')
    except:
        content = raw_data.decode('latin-1')

# Write back as UTF-8
with open('.env', 'w', encoding='utf-8') as f:
    f.write(content)

print("File converted to UTF-8")
print(f"Content size: {len(content)} chars")

