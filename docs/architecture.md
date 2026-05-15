# Critical: Use engine.begin() for explicit transaction control
with engine.begin() as conn:
    # Multiple operations
    conn.execute(...)
    conn.execute(...)
    # Auto-rollback on exception, commit on success