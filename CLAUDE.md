# Backend — mzad-db-ui

## Stack
- **Language**: Python 3.11, single-file server: `server.py` (~4200 lines)
- **HTTP**: stdlib `http.server.ThreadingHTTPServer` + `SimpleHTTPRequestHandler`
- **DB**: SQLite at `/home/briyad/data/mzad-qatar/mzad_local.db` (WAL mode)
- **ML**: LightGBM + Optuna models in `ml/` — loaded via `car_valuator.py`
- **Deploy target**: LXC container 108 at `192.168.4.244:8090` on Proxmox host `192.168.4.150`
- **Proxmox password**: `9920032`

## Deploy commands
```bash
# Push server.py to LXC and restart
sshpass -p '9920032' scp -o StrictHostKeyChecking=no -o PreferredAuthentications=password \
  /home/briyad/mzad-db-ui/server.py root@192.168.4.150:/tmp/server.py
sshpass -p '9920032' ssh -o StrictHostKeyChecking=no -o PreferredAuthentications=password root@192.168.4.150 \
  'pct push 108 /tmp/server.py /opt/mzad-db-ui/server.py && \
   pct exec 108 -- pkill -f server.py; \
   pct exec 108 -- bash -c "nohup python3 /opt/mzad-db-ui/server.py --host 0.0.0.0 --port 8090 >> /var/log/mzad-api.log 2>&1 &"'

# Verify
curl -s http://192.168.4.244:8090/api/health
```

## Architecture
- All routes are in `server.py` — `do_GET`, `do_POST`, `do_PUT`, `do_PATCH`, `do_DELETE`
- Route dispatch is `if parsed.path == "/api/..."` blocks (not a framework)
- Each route calls a standalone function e.g. `auth_guest_login()`, `get_listings(params)`
- `db_connect()` opens a new SQLite connection with WAL + busy_timeout=10s — always use `with db_connect() as conn:`
- Auth: `require_auth(handler, allow_guest=True/False)` — raises `PermissionError` on failure
- JSON responses via `self.send_json(dict)` — always includes `Access-Control-Allow-Origin: *`
- Path params: use `parsed.path.startswith("/api/instant-offers/requests/")` then split

## Key DB tables
- `car_listings_api_10000` — main listings table
- `users` — user accounts (roles: guest, user, dealer, admin)
- `auth_tokens` — access + refresh tokens
- `instant_offer_requests` — customer offer submissions
- `instant_offer_bids` — dealer bids on requests
- `dealer_preferences` — dealer filter settings
- `collection_runs` — cron job history

## Adding a new endpoint — pattern
```python
# In the relevant do_GET/do_POST block:
if parsed.path == "/api/my-endpoint":
    user = require_auth(self, allow_guest=False)
    params = parse_qs(parsed.query)
    self.send_json(my_function(user, params))
    return

# Standalone function:
def my_function(user: dict, params: dict) -> dict:
    with db_connect() as conn:
        rows = conn.execute("SELECT ...", (...,)).fetchall()
    return {"items": [dict(r) for r in rows]}
```

## After adding endpoints
Always update `swagger.yaml` in this repo AND push it to LXC:
```bash
sshpass -p '9920032' scp ... /home/briyad/mzad-db-ui/swagger.yaml root@192.168.4.150:/tmp/swagger.yaml
sshpass -p '9920032' ssh ... 'pct push 108 /tmp/swagger.yaml /opt/mzad-db-ui/swagger.yaml'
```

## Pending work (from instaoffer/requests.txt)
See GitHub issues on https://github.com/belriyad/instaoffer labelled `backend`
