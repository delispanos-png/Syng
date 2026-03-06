# WHMCS <-> GoodDay Sync

Canonical runtime location:
- `/opt/pharmacyone/whmcs-goodday-sync`

## Project Structure
- `docker-compose.yml` - service orchestration
- `Dockerfile` - image build
- `requirements.txt` - Python deps
- `whmcs-goodday-sync/app.py` - sync engine
- `whmcs-goodday-sync/.env` - runtime config
- `whmcs-goodday-sync/state.json` - persistent sync state

## Documentation
- Technical handbook: [`TECHNICAL_HANDBOOK.md`](/opt/pharmacyone/whmcs-goodday-sync/TECHNICAL_HANDBOOK.md)
- Team operations: [`TEAM_OPERATIONS.md`](/opt/pharmacyone/whmcs-goodday-sync/TEAM_OPERATIONS.md)
- Command runbook: [`whmcs-goodday-sync/RUNBOOK.md`](/opt/pharmacyone/whmcs-goodday-sync/whmcs-goodday-sync/RUNBOOK.md)
# Syng
