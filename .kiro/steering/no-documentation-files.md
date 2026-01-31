---
inclusion: always
---

# Documentation File Creation Policy

## CRITICAL RULE: Do NOT create documentation files unless explicitly requested

You MUST NOT create any of the following file types unless the user explicitly asks for them:

- ❌ `.md` files (markdown documentation)
- ❌ `.docx` files (Word documents)
- ❌ `.txt` files for documentation/summaries
- ❌ `README.md` files
- ❌ `SUMMARY.md` files
- ❌ `*_REMOVAL.md` files
- ❌ `*_IMPLEMENTATION.md` files
- ❌ Any other documentation files

## What you SHOULD do instead:

1. **Make the code changes** the user requested
2. **Provide a brief summary in chat** (2-3 sentences max)
3. **Stop there** - do not create files to document your work

## When you CAN create documentation files:

Only when the user explicitly says:
- "Create a document about..."
- "Write a README for..."
- "Document this in a file..."
- "Make a summary file..."

## Examples:

### ❌ WRONG (Do NOT do this):
```
User: "Remove the validate_data task"
You: 
  1. Remove the task ✓
  2. Create VALIDATE_DATA_REMOVAL.md ✗ (NOT REQUESTED!)
  3. Create summary document ✗ (NOT REQUESTED!)
```

### ✅ CORRECT (Do this):
```
User: "Remove the validate_data task"
You:
  1. Remove the task ✓
  2. Brief chat summary: "Removed validate_data task from maintenance.py and __init__.py. Task was not critical and only counted valid/invalid products without fixing errors."
  3. Done! (No files created)
```

## Summary:

- **Default behavior:** Make code changes + brief chat summary
- **Documentation files:** Only when explicitly requested
- **Keep it simple:** Less is more

This rule applies to ALL interactions unless the user specifically asks for documentation files.
