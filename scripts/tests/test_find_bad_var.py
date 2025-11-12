from dotenv import dotenv_values
import os

config = dotenv_values('.env')
keys_list = list(config.items())

for i, (k, v) in enumerate(keys_list):
    if v is None:
        print(f"Variable at index {i} has None value:")
        print(f"  Key: {repr(k)}")
        print(f"  Value: None")
        break
    if not isinstance(v, str):
        print(f"Variable at index {i} has non-string value:")
        print(f"  Key: {repr(k)}")
        print(f"  Value type: {type(v)}")
        print(f"  Value: {repr(v)}")
        break
    try:
        os.environ[k] = v
    except (ValueError, TypeError) as e:
        print(f"Error at index {i}:")
        print(f"  Key: {repr(k)}")
        print(f"  Value: {repr(v)[:100]}")
        print(f"  Error: {e}")
        break

print("Variables after problem ones:")
# Show all remaining variables
for i, (k, v) in enumerate(keys_list):
    if i >= 44:  # Start from the bad one
        print(f"{i}: {k} = {repr(v) if v else 'None'}")
