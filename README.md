# Kiarolabs Learning Intelligence Service

This repository contains the **offline AI / analytics layer** for WordSprint.

## Hard Rules
- No UI code
- No real-time execution
- No cross-app database access
- Reads only from app-local tables (spelling_*, math_*, synonym_*)
- Writes only to *_ai_* tables

If this service is OFF, all learning apps must continue to work normally.
