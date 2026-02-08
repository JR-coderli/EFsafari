# Repository Guidelines

> 本仓库约定：助手与自动化工具的回复一律使用中文。

## Project Structure & Module Organization
- `backend/` holds the FastAPI service, ETL code, and ClickHouse/Redis integration. Key entrypoints are `backend/api/main.py` (API) and `backend/run_etl.py` (ETL runner).
- `EflowJRbi/` contains the React + Vite frontend. UI logic lives in `EflowJRbi/src/` and shared types in `EflowJRbi/types.ts`.
- `deploy/` includes deployment and update scripts.
- Root docs: `README.md`, `backend/API_README.md`, and `backend/ETL_README.md` explain runtime behavior and ETL details.

## Build, Test, and Development Commands
- Frontend dev server: `cd EflowJRbi; npm install; npm run dev` (Vite hot reload).
- Frontend build: `cd EflowJRbi; npm run build` (production bundle), `npm run preview` (local preview).
- Backend dev server (simple): `cd backend; python -m uvicorn api.main:app --host 0.0.0.0 --port 8001 --reload`.
- Backend dev server (scripted): `cd backend; ./start_dev.sh` (kills port conflicts, runs multiple workers).
- ETL runner (manual): `cd backend; python run_etl.py`.

## Coding Style & Naming Conventions
- Python: 4-space indentation, follow existing module layout under `backend/api/` and `backend/clickflare_etl/`.
- TypeScript/React: 2-space indentation in `.ts/.tsx`, keep imports grouped as in existing files.
- No enforced formatter or linter is configured; match the surrounding file style.

## Testing Guidelines
- There is no formal test framework configured. Use `backend/test_api_return.py` for ad?hoc API/ClickHouse checks when needed.
- If you add tests, document how to run them in this file and update relevant READMEs.

## Commit & Pull Request Guidelines
- Commit messages follow a Conventional Commit?style prefix: `feat:`, `fix:`, `refactor:`, `style:`.
- Some commits include a timestamp prefix (e.g., `2026��02��06�� 23:41:21 feat: ...`). Either format is acceptable; be consistent within a series.
- PRs should include: summary of changes, how to run locally, and screenshots for UI changes. Link related issues if applicable.

## Backend Model Update Checklist
- When adding fields to API responses, update all of:
  - `backend/api/models/schemas.py`
  - `backend/api/database.py` (formatters)
  - `backend/api/routers/dashboard.py` (query logic)

## Configuration Tips
- Service configuration is stored in `backend/api/config.yaml`. Verify ClickHouse/Redis settings before running ETL or API locally.
