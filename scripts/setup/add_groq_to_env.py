groq_config = """

# ============================================
# GROQ API CONFIGURATION
# ============================================
GROQ_API_KEY=
GROQ_API_KEYS=
GROQ_MODEL=llama-3.1-70b-versatile
GROQ_BASE_URL=https://api.groq.com/openai/v1
"""

with open('.env', 'a', encoding='utf-8') as f:
    f.write(groq_config)

print("Added Groq configuration to .env")
print("\n!!! IMPORTANT !!!")
print("Please edit .env and add your Groq API keys:")
print("  GROQ_API_KEY=your_single_key")
print("  OR")
print("  GROQ_API_KEYS=key1,key2,key3")

