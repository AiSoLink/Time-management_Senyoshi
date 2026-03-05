# Backend (FastAPI)
API:
- GET/POST /api/companies
- GET /api/companies/{company}/devices
- POST /api/jobs (multipart: company/device + pdfs[])
- GET /api/jobs/{jobId}
- GET /api/jobs/{jobId}/download/{excel|log|skipped}

永続化:
- work/jobs/<jobId>/input
- work/jobs/<jobId>/output
- work/jobs/<jobId>/state.json
