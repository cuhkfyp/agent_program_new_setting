# Security

- No credentials, session cookies, site configuration, client extracts, or
  client logs belong in this repository.
- Server mutation methods require `ccd-user` or `System Manager` and validate
  registration, stable source, run, and lease ownership.
- Lease tokens are random/idempotent capabilities stored only in Redis and
  process memory; state fields never display them.
- The agent reads ERPNext credentials from the OS keyring. Runtime endpoint and
  username values come from environment variables.
- Client database passwords are retrieved through the existing authorized API,
  kept in memory/keyring for the daemon, and never written to logs.
- The fast API rejects unknown fields, child-table fields, cross-source rows,
  missing source keys, oversized batches, and unsupported naming rules.

Report a suspected leak by rotating the affected secret first, then removing it
from both current files and Git history before sharing the repository further.
