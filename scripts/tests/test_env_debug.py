from dotenv import dotenv_values

try:
    config = dotenv_values('.env')
    for k, v in list(config.items())[:10]:
        print(f"{k} = {repr(v)}")
        if v and '\x00' in v:
            print(f"  -> Contains null byte!")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

