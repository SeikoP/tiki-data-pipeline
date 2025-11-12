import os
from dotenv import load_dotenv

print(f"Current env size: {len(os.environ)}")

# Check for null bytes in current env
bad_keys = []
for k, v in list(os.environ.items()):
    try:
        if v and '\x00' in v:
            bad_keys.append(k)
    except:
        pass

if bad_keys:
    print(f"Found {len(bad_keys)} vars with null bytes: {bad_keys[:5]}")
    for k in bad_keys:
        del os.environ[k]
        print(f"  Removed {k}")

print("Loading .env...")
load_dotenv('.env', override=True)

print("After load_dotenv - checking GROQ config:")
print(f"GROQ_API_KEY: {os.getenv('GROQ_API_KEY')}")
print(f"GROQ_API_KEYS: {os.getenv('GROQ_API_KEYS')}")

