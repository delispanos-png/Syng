# WHMCS <-> GoodDay Sync Runbook

## Canonical Paths
- Project: `/opt/pharmacyone/whmcs-goodday-sync`
- Compose: `/opt/pharmacyone/whmcs-goodday-sync/docker-compose.yml`
- App: `/opt/pharmacyone/whmcs-goodday-sync/whmcs-goodday-sync/app.py`
- Env: `/opt/pharmacyone/whmcs-goodday-sync/whmcs-goodday-sync/.env`
- State: `/opt/pharmacyone/whmcs-goodday-sync/whmcs-goodday-sync/state.json`
- Container: `whmcs-goodday-sync`

## Deploy
```bash
docker compose -f /opt/pharmacyone/whmcs-goodday-sync/docker-compose.yml up -d --build whmcs-goodday-sync
```

## Status / Health
```bash
docker compose -f /opt/pharmacyone/whmcs-goodday-sync/docker-compose.yml ps
docker inspect --format='{{json .State.Health}}' whmcs-goodday-sync
```

## Logs
```bash
docker compose -f /opt/pharmacyone/whmcs-goodday-sync/docker-compose.yml logs --since=10m whmcs-goodday-sync
```

## Fast Filters
```bash
# ticket specific
docker compose -f /opt/pharmacyone/whmcs-goodday-sync/docker-compose.yml logs --since=30m whmcs-goodday-sync | grep 'Ticket 2368'

# ticket specific (public tid/internal ticketid mismatch check)
docker compose -f /opt/pharmacyone/whmcs-goodday-sync/docker-compose.yml logs --since=30m whmcs-goodday-sync | grep -E '2369|303946'

# delete events
docker compose -f /opt/pharmacyone/whmcs-goodday-sync/docker-compose.yml logs --since=30m whmcs-goodday-sync | grep -E 'Deleted WHMCS reply|Deleted WHMCS ticket'
```

## Safety Profile (Production Default)
Current safe defaults:
- `SYNC_DELETE_TICKETS_GOODDAY_TO_WHMCS=0`
- `GOODDAY_TASK_DELETE_CONFIRMATION_HITS=3`
- `GOODDAY_TASK_DELETE_CHECK_PROJECT_LIST=0`
- `GOODDAY_TASK_DELETE_PROJECT_LIST_ON_META_ERROR=0`

Quick verify:
```bash
rg -n "SYNC_DELETE_TICKETS_GOODDAY_TO_WHMCS|GOODDAY_TASK_DELETE_CONFIRMATION_HITS|GOODDAY_TASK_DELETE_CHECK_PROJECT_LIST|GOODDAY_TASK_DELETE_PROJECT_LIST_ON_META_ERROR" \
  /opt/pharmacyone/whmcs-goodday-sync/whmcs-goodday-sync/.env
```

Post-deploy guard check (must be empty for full-ticket deletes):
```bash
docker compose -f /opt/pharmacyone/whmcs-goodday-sync/docker-compose.yml logs --since=30m whmcs-goodday-sync | grep 'Deleted WHMCS ticket'
```

## Webhook Quick Test
```bash
curl -sS -X POST "http://127.0.0.1:8787/webhook" \
  -H "Content-Type: application/json" \
  -H "X-Webhook-Secret: <SYNC_WEBHOOK_SECRET>" \
  -H "X-Event-ID: test-2363-1" \
  -d '{"event":"ticket_created","ticketid":"2363"}'
```

## Docs
- Technical handbook: `/opt/pharmacyone/whmcs-goodday-sync/TECHNICAL_HANDBOOK.md`
- Team operations: `/opt/pharmacyone/whmcs-goodday-sync/TEAM_OPERATIONS.md`
