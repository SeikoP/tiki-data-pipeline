from dotenv import dotenv_values
import os

print("Loading with dotenv_values (doesn't set env)...")
config = dotenv_values('.env')
print(f"Loaded {len(config)} variables")

# Check which variable causes the issue
from dotenv.main import DotEnv
print("\nTrying to set env variables one by one...")
for i, (k, v) in enumerate(config.items()):
    try:
        os.environ[k] = v
        print(f"OK {k}")
    except ValueError as e:
        print(f"FAIL {k} = {repr(v)[:50]}... -> {e}")
        if i > 10:  # Stop after a few
            break

