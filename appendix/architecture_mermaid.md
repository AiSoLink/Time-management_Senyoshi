# アーキテクチャ図（Mermaid）
```mermaid
flowchart LR
  subgraph FE[Next.js (React/TypeScript)]
    A[UI] --> A1[Upload PDFs]
    A --> A2[Job Status]
    A2 --> A3[Download Excel/Logs]
  end

  subgraph BE[FastAPI (Python)]
    B[API] --> B1[Job Runner]
    B1 --> B2[Extract Header (pdfplumber)]
    B1 --> B3[Extract Details (camelot)]
    B1 --> B4[Aggregate -> Excel (openpyxl)]
    B1 --> B5[Write Logs]
    B1 --> B6[state.json]
  end

  A1 -->|multipart| B
  A2 <--> B
  A3 <--> B
```
