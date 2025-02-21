# gel_auth

Contains authentication routines for all supported auth methods for PostgreSQL and EdgeDB:

| Auth Method | PG | Gel | Notes |
|-------------|----|-----|-------|
| Plaintext   | ✓  |     |       |
| MD5         | ✓  |     |       |
| SCRAM       | ✓  | ✓   | `SCRAM-SHA-256` only |

