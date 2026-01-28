"""Tiki crawl products DAG package (refactored).

This package isolates:
- bootstrap/config/import-path setup
- task callables (business logic)
- DAG wiring (TaskGroups + dependencies)

Behavior, task_ids, and dependencies are preserved from the legacy monolithic DAG.
"""
