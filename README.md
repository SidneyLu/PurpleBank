# PurpleBank -- A Nucleotide Sequence Database 
> This is the final project for course *Database Systems* at NKU-CS.
> 
> PurpleBank is designed according to the schematic of NCBI-Gene, with basic
information management and retrieval functions available.


## Tech Stack
- Backend: MySQL + FastAPI
- Host Language: Python
- Frontend: Next.js + React

## Quick Start
### Backend with uv (recommended)
```powershell
cd D:\1000\apps\backend
uv sync --extra dev
Copy-Item .env.example .env
uv run uvicorn app.main:app --host 127.0.0.1 --port 8001 --reload
```

If you do not want `uv`, you can still use:
```powershell
pip install -r requirements.txt
```

### Build from raw data (new user path)
1. Initialize database schema in order:
   - `db/purple_bank_01_database.sql`
   - `db/purple_bank_02_core_schema.sql`
   - `db/purple_bank_03_extension_schema.sql`
   - `db/purple_bank_04_indexes.sql`
   - `db/purple_bank_05_seed_data.sql`
   - `db/purple_bank_06_triggers.sql`
   - `db/purple_bank_07_views.sql`
   - `db/purple_bank_08_procedures.sql`
   - `db/purple_bank_09_user_change_request.sql`
2. Install data import dependencies:
```powershell
cd D:\1000\data\scripts
python -m pip install -r requirements.txt
```
3. Import FASTA/XML into MySQL:
```powershell
python insert.py --xml ..\raw\gene_result.xml --fasta ..\raw\gene_result_all_genes.fasta --host 127.0.0.1 --user root --password <YOUR_PASSWORD> --database purple_bank
```
4. Optional: enrich `reference/author` metadata from PubMed:
```powershell
python reference.py --host 127.0.0.1 --user root --password <YOUR_PASSWORD> --database purple_bank
```

### Frontend 

```bash
cd apps/frontend
npm install
copy .env.example .env.local
npm run dev
```

Open: `http://127.0.0.1:3000`


## Repository Structure
- `db/`: modular SQL scripts
- `apps/backend/`: FastAPI API server
- `apps/frontend/`: Next.js web app
- `data/`: data scripts and raw files

## Core API

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
