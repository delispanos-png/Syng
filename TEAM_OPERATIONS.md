# WHMCS <-> GoodDay Sync - Team Operations Guide

## 1. Team Structure
### Service Owner (L3)
- Αποφασίζει για architecture/config αλλαγές.
- Εγκρίνει production deployments.
- Κάνει root-cause analysis σε κρίσιμα incidents.

### Integrations Engineer (L2)
- Χειρίζεται sync mappings, retries, webhook, state anomalies.
- Εκτελεί deploy/restart.
- Υλοποιεί fixes και επιβεβαιώνει με end-to-end tests.

### Support Operator (L1)
- Παρακολουθεί health/logs.
- Εκτελεί βασικό runbook.
- Κλιμακώνει σε L2 όταν υπάρχει delay/failure > SLA.

## 2. Daily Checklist (L1)
1. Service health:
```bash
docker compose -f /opt/pharmacyone/whmcs-goodday-sync/docker-compose.yml ps
```
2. Recent errors:
```bash
docker compose -f /opt/pharmacyone/whmcs-goodday-sync/docker-compose.yml logs --since=15m whmcs-goodday-sync
```
3. Verify webhook endpoint reachable (`:8787/healthz`).
4. Spot-check one recent WHMCS->GoodDay and one GoodDay->WHMCS sync.

## 3. SLAs
- New reply sync target: 5-30s (normal network).
- Delete reply sync target: 10-45s.
- Full ticket delete sync target: N/A in production default (feature disabled by safety policy).
- Incident threshold: >2 συνεχόμενα λεπτά χωρίς πρόοδο σε ενεργό test.

## 4. Incident Runbook
### A. Reply delete δεν περνάει
1. Βρες `ticketid`, `tid`, `task_id`.
2. Έλεγξε logs για:
- `Deleted WHMCS reply ...`
- `delete stale mapping cleared ...`
- `GoodDay public -> WHMCS sync failed ...`
3. Αν υπάρχουν timeouts, περίμενε 1-2 κύκλους και επανέλεγξε.
4. Αν επιμένει > SLA, κλιμάκωση σε L2.

### B. Full ticket delete δεν περνάει
1. Επαλήθευσε πρώτα ότι η δυνατότητα είναι όντως ενεργή (`SYNC_DELETE_TICKETS_GOODDAY_TO_WHMCS=1`).
2. Αν η δυνατότητα είναι disabled (production default), το expected behavior είναι να **μην** διαγράφεται WHMCS ticket.
3. Αν εμφανιστεί απροσδόκητο `Deleted WHMCS ticket ...`, άμεση κλιμάκωση σε L2/L3 (critical incident).

### C. Service restart loop
1. `docker compose ... ps`
2. `docker compose ... logs --since=5m`
3. Συνήθη αιτία: transient external timeout στο startup.
4. Αν συνεχίζει restart loop > 2 λεπτά, L2 άμεσα.

## 5. Deployment Checklist (L2)
1. Validate code (`python3 -m py_compile app.py`).
2. Deploy:
```bash
docker compose -f /opt/pharmacyone/whmcs-goodday-sync/docker-compose.yml up -d --build whmcs-goodday-sync
```
3. Verify healthy.
4. Run 4 acceptance tests:
- New ticket
- Edit from WHMCS
- Edit from GoodDay `!public`
- Delete reply

Optional (μόνο σε controlled test profile):
- Full ticket delete
5. Record deployment timestamp + result.

## 6. Access & Secrets
- Secrets only in `.env`.
- No secret sharing via chat/screenshots.
- Rotate WHMCS/GoodDay tokens if leakage suspected.

## 7. Ownership Matrix
- L1: monitoring + first response
- L2: functional fixes + config tuning
- L3: architectural decisions + incident postmortem

## 8. Handover Notes Template
Για κάθε αλλαγή:
- What changed
- Why
- Exact files
- Validation evidence (log lines + timestamps)
- Rollback method
