# Security Policy

## Reporting Security Vulnerabilities
If you discover a security vulnerability, please email security@solvist.io instead of using GitHub issues.

## Current Security Measures
- JWT-based authentication via Supabase Auth
- Row Level Security (RLS) on all database tables
- CORS whitelist for verified domains
- HTTPS enforced on all endpoints
- Secrets stored in Render Secret Store (ENGINE_SECRET, SUPABASE_SERVICE_ROLE_KEY)

## Endpoints Requiring Authentication
All endpoints under /api/* require Bearer JWT token except:
- POST /api/signup, /api/login (public)
- GET /health, /version (informational)

## Database Security
- Multi-tenant isolation via company_id filtering
- SECURITY DEFINER function for RLS-safe tenant lookup
- No raw SQL queries (all parameterized via Pydantic/SQLAlchemy)

## Dependencies
Dependencies are pinned in requirements.txt. Run `pip check` to identify vulnerable versions.

## Changelog
- 2026-04-02: Initial security policy
