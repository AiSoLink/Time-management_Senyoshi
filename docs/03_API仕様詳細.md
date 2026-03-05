# API仕様（詳細）
版：2.0  作成日：2026-02-16 09:37:28

---

## GET /api/companies
200:
```json
{ "companies": ["__SAMPLE_COMPANY__", "A社"] }
```

## POST /api/companies
Request:
```json
{ "name": "A社" }
```
201:
```json
{ "name": "A社", "created": true }
```

## GET /api/companies/{company}/devices
200:
```json
{ "company":"A社", "devices":[{"name":"mimamori","preset":true},{"name":"telecom","preset":false}] }
```

## POST /api/jobs（multipart/form-data）
fields：company, device  
files：pdfs（複数）

202:
```json
{ "jobId":"20260216_123045_abcd" }
```

## GET /api/jobs/{jobId}
200: 外部仕様書参照

## ダウンロード
- GET /api/jobs/{jobId}/download/excel（xlsx）
- GET /api/jobs/{jobId}/download/log（csv）
- GET /api/jobs/{jobId}/download/skipped（json）
