# PurpleBank - Nucleotide Sequence Database WebAPP

PurpleBank is a course project for *Database Systems* at NKU-CS.
This repository now contains a runnable full-stack WebAPP with:

- Backend: FastAPI
- Database: MySQL
- Frontend: Next.js + React (App Router)

## Implemented Product Rules

- Guests can browse search + sequence detail pages without login.
- Normal users can query data and submit mutation requests.
- Admins can query, submit requests, review requests, and manage users.
- All CREATE/UPDATE/DELETE operations require review before execution.
- Admin user create/delete also goes through review workflow.
- User deletion is logical deletion (`app_user.is_active = 0`).

## Repository Structure

- `db/`: modular SQL scripts
- `apps/backend/`: FastAPI API server
- `apps/frontend/`: Next.js web app
- `data/`: data scripts and raw files

## Database Setup

Run SQL scripts in order:

1. `db/purple_bank_01_database.sql`
2. `db/purple_bank_02_core_schema.sql`
3. `db/purple_bank_03_extension_schema.sql`
4. `db/purple_bank_04_indexes.sql`
5. `db/purple_bank_05_seed_data.sql`
6. `db/purple_bank_06_triggers.sql`
7. `db/purple_bank_07_views.sql`
8. `db/purple_bank_08_procedures.sql`
9. `db/purple_bank_09_user_change_request.sql`

## Backend Run

```bash
cd apps/backend
python -m venv .venv
# Windows
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
uvicorn app.main:app --reload --host 127.0.0.1 --port 8001
```

Health check:

```bash
curl http://127.0.0.1:8001/health
```

## Frontend Run

```bash
cd apps/frontend
npm install
copy .env.example .env.local
npm run dev
```

Open: `http://127.0.0.1:3000`

## Core API Surface

- Public:
  - `GET /api/v1/sequences/search`
  - `GET /api/v1/sequences/{accession}`
- Auth:
  - `POST /api/v1/auth/register`
  - `POST /api/v1/auth/login`
  - `GET /api/v1/auth/me`
- Sequence workflow:
  - `POST /api/v1/sequence-requests`
  - `GET /api/v1/sequence-requests/my`
  - `GET /api/v1/admin/sequence-requests`
  - `POST /api/v1/admin/sequence-requests/{id}/review`
- User workflow (admin):
  - `GET /api/v1/admin/users`
  - `POST /api/v1/admin/user-requests`
  - `GET /api/v1/admin/user-requests`
  - `POST /api/v1/admin/user-requests/{id}/review`

## Default Accounts

`db/purple_bank_05_seed_data.sql` seeds:

- admin / (pre-hashed password in SQL)
- user / (pre-hashed password in SQL)

You can also register new `user` accounts via API/UI.

## Tests

Backend:

```bash
cd apps/backend
pytest
```

Frontend e2e:

```bash
cd apps/frontend
npm run test:e2e
```
