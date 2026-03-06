# WHMCS <-> GoodDay Sync - Technical Handbook

## 1. Scope
Το service συγχρονίζει tickets/replies μεταξύ WHMCS και GoodDay με bidirectional λογική:
- WHMCS -> GoodDay: ticket create, reply create/edit/delete.
- GoodDay -> WHMCS: `!public` replies create/edit/delete.
- Full ticket delete: προαιρετικό feature (GoodDay task delete -> WHMCS ticket delete), currently disabled by default in production.

## 2. Canonical Runtime Layout
- Root: `/opt/pharmacyone/whmcs-goodday-sync`
- Compose: `/opt/pharmacyone/whmcs-goodday-sync/docker-compose.yml`
- Service code: `/opt/pharmacyone/whmcs-goodday-sync/whmcs-goodday-sync/app.py`
- Runtime env: `/opt/pharmacyone/whmcs-goodday-sync/whmcs-goodday-sync/.env`
- Runtime state: `/opt/pharmacyone/whmcs-goodday-sync/whmcs-goodday-sync/state.json`
- Container: `whmcs-goodday-sync`

## 3. Runtime Architecture
- Single Python process (`app.py`) με polling loop + embedded webhook server.
- HTTP stack:
  - `SESSION` (with retry) για safe reads.
  - `SESSION_NO_RETRY` για write/idempotency-sensitive calls.
- Stateful sync via `state.json`:
  - mapping `ticket_id <-> task_id`
  - reply/message mapping
  - signatures για edit detection
  - delete guard tombstones για anti-recreate προστασία.

## 4. Core Sync Rules
### 4.1 WHMCS -> GoodDay
- Νέο WHMCS ticket δημιουργεί GoodDay task.
- Νέα WHMCS reply δημιουργεί GoodDay comment.
- Edit WHMCS reply κάνει update στο υπάρχον GoodDay message (όχι νέο, όταν υπάρχει mapping).
- Delete WHMCS reply αφαιρεί mapped GoodDay message.

### 4.2 GoodDay -> WHMCS
- Μόνο μηνύματα με prefix `!public` περνάνε σε WHMCS reply.
- Edit `!public` update-άρει το mapped WHMCS reply.
- Delete/soft-delete `!public` message διαγράφει WHMCS reply.

### 4.3 Full Ticket Delete (GoodDay -> WHMCS)
Το full delete είναι safety-sensitive και **στο production profile είναι disabled**.

Όταν ενεργοποιηθεί, γίνεται μόνο με αυστηρά guardrails:
- Confirmed missing από `task/{id}` metadata ή explicit fallback policy.
- Πολλαπλά confirmation hits πριν από `DeleteTicket`.
- Προστασία από fallback σε meta-timeout unless explicit opt-in.

Safety defaults (code + env):
- `SYNC_DELETE_TICKETS_GOODDAY_TO_WHMCS=0`
- `GOODDAY_TASK_DELETE_CONFIRMATION_HITS=3`
- `GOODDAY_TASK_DELETE_CHECK_PROJECT_LIST=0`
- `GOODDAY_TASK_DELETE_PROJECT_LIST_ON_META_ERROR=0`

## 5. Required Environment (minimum)
- `WHMCS_API_URL`
- `WHMCS_API_IDENTIFIER`
- `WHMCS_API_SECRET`
- `WHMCS_ADMIN_USERNAME` (required για GoodDay -> WHMCS writes)
- `GOODDAY_API_TOKEN`
- `GOODDAY_FROM_USER_ID`
- `GOODDAY_PROJECT_ID` ή `GOODDAY_PROJECT_BY_DEPARTMENT_JSON`

## 6. Recommended Production Profile (current)
- `WHMCS_POLL_SECONDS=5`
- `SYNC_WEBHOOK_ENABLED=1`
- `SYNC_WEBHOOK_TARGETED=1`
- `SYNC_WEBHOOK_TARGETED_GD_ONLY=0`
- `SYNC_GOODDAY_TO_WHMCS=1`
- `SYNC_DELETES_GOODDAY_TO_WHMCS=1`
- `SYNC_DELETE_TICKETS_GOODDAY_TO_WHMCS=0`
- `GOODDAY_TASK_DELETE_CONFIRMATION_HITS=3`
- `GOODDAY_TASK_DELETE_CHECK_META=1`
- `GOODDAY_TASK_DELETE_CHECK_PROJECT_LIST=0`
- `GOODDAY_TASK_DELETE_PROJECT_LIST_ON_META_ERROR=0`
- `GOODDAY_TASK_META_TIMEOUT_CONTINUE=1`
- `WHMCS_DELETE_RECREATE_GUARD_SECONDS=900`

Timeout/backoff (fast-delete profile):
- `GOODDAY_TASK_CONNECT_TIMEOUT=6`
- `GOODDAY_TASK_HTTP_TIMEOUT=8`
- `GOODDAY_TASK_RETRIES=2`
- `GOODDAY_MESSAGES_CONNECT_TIMEOUT=6`
- `GOODDAY_MESSAGES_HTTP_TIMEOUT=8`
- `GOODDAY_MESSAGES_RETRIES=1`
- `GD_TO_WHMCS_FAILURE_BACKOFF_SECONDS=1`
- `GD_TO_WHMCS_TIMEOUT_BACKOFF_SECONDS=3`

## 7. Build / Deploy / Restart
```bash
docker compose -f /opt/pharmacyone/whmcs-goodday-sync/docker-compose.yml up -d --build whmcs-goodday-sync
```

## 8. Health and Logs
```bash
docker compose -f /opt/pharmacyone/whmcs-goodday-sync/docker-compose.yml ps
docker inspect --format='{{json .State.Health}}' whmcs-goodday-sync
docker compose -f /opt/pharmacyone/whmcs-goodday-sync/docker-compose.yml logs --since=10m whmcs-goodday-sync
```

Critical success patterns:
- `Created GoodDay task for ticket ...`
- `Synced GoodDay public msg ... -> WHMCS ticket ...`
- `Deleted WHMCS reply ...`

Optional pattern (only if full-ticket delete is intentionally enabled):
- `Deleted WHMCS ticket ... because mapped GoodDay task was removed ...`

## 9. Known Failure Modes
1. WHMCS API timeouts (`my.cloudon.gr`) -> delayed cycles.
2. GoodDay task meta returns `null` / timeouts -> delayed GoodDay->WHMCS checks (no full-ticket delete in current safety profile).
3. Webhook not arriving -> fallback polling still handles sync with delay.
4. Missing mapping after restarts -> state recovery path tries to rebuild.

## 10. Troubleshooting Procedure
1. Map ticket:
- WHMCS public number (`tid`) vs internal `ticketid`.
- GoodDay task title uses internal `ticketid` (`[WHMCS #<ticketid>]`), while custom field may store public `tid`.
- GoodDay task id from state.
2. Check state:
```bash
jq '.tickets["<ticketid>"]' /opt/pharmacyone/whmcs-goodday-sync/whmcs-goodday-sync/state.json
```
3. Check logs for same `ticketid`.
4. Validate GoodDay endpoints (`task/{id}`, `task/{id}/messages`, `project/{id}/tasks`).
5. If config changed: rebuild/restart container.

## 11. Backup / Restore
- Backup `state.json` πριν από μεγάλα refactors.
- Restore: stop service, replace `state.json`, start service.
- Hard-deleted WHMCS ticket (`DeleteTicket`) δεν επαναφέρεται από το sync service. Απαιτείται restore από WHMCS DB backup.

## 12. Security
- Keep `.env` permissions strict (`600` for secrets).
- Never expose `GOODDAY_API_TOKEN`, `WHMCS_API_SECRET` in logs.
- Protect webhook with `X-Webhook-Secret`.

## 13. Change Management
- Κάθε αλλαγή σε sync logic πρέπει να περνάει:
  1. New ticket test
  2. Edit test (both directions)
  3. Delete reply test (both directions)
  4. Full ticket delete test (GoodDay -> WHMCS) μόνο σε controlled profile όπου είναι ενεργό το feature
- Μετά από deploy, monitor 10-15 λεπτά logs για regressions.
