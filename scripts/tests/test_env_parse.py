with open('.env', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    line_stripped = line.rstrip('\n\r')
    if line_stripped and not line_stripped.startswith('#'):
        # Check for suspicious characters
        if '\x00' in line_stripped:
            print(f"Line {i+1}: Contains null byte!")
        if '=' in line_stripped:
            key, val = line_stripped.split('=', 1)
            if '\x00' in val:
                print(f"Line {i+1}: Value contains null byte!")
                print(f"  Key: {repr(key)}")
                print(f"  Val: {repr(val)}")
            # Check for problematic values
            if val == '' or val.isspace():
                print(f"Line {i+1}: Empty value: {key}=")
            # Check GROQ related
            if 'GROQ' in key:
                print(f"Line {i+1}: {key}={repr(val)}")

