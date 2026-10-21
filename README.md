# Vyavahr WhatsApp gateway

Sends bills, payment reminders and CA document requests from each shop's (or CA firm's) own WhatsApp number.
The app writes messages into `wa.outbox` and linking commands into `wa.sessions` (Supabase migration 045);
this service holds one WhatsApp Web session per linked number and sends one message at a time with a short,
randomised gap.

## Run on Render (free)
1. Push this folder to a GitHub repository.
2. Render > New > Blueprint > pick the repository. It reads `render.yaml`.
3. Set `DATABASE_URL` to the Supabase **session** pooler URL (port 5432) of the Vyavahr project.
4. Deploy. `GET /health` shows `{ok, active, sessions, max}`. On boot it registers a pg_cron job that pings
   `/health` every 10 minutes so the free instance never sleeps.

Then in Vyavahr: Settings > WhatsApp > Link with QR code (shops on Pro or Business), or Firm > WhatsApp (CA partners).

## Capacity
One linked number uses about 40-80 MB. The free instance (512 MB) holds about 5 (`MAX_SESSIONS=5`); others wait
with a message on their screen. Move to a bigger instance and raise `MAX_SESSIONS` as shops link their numbers.

## Run on a PC instead
Put `DATABASE_URL=...` in a `.env` file next to `index.js`, then `npm install` and `npm start`.
Only one copy is ever active (Postgres advisory lock); a second copy waits on standby.
