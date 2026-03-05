"use client";
import { useEffect, useState } from "react";
import { apiGet, apiPostJson } from "@/components/api";

type CompaniesRes = { companies: string[] };

export default function Page() {
  const [companies, setCompanies] = useState<string[]>([]);
  const [name, setName] = useState("");
  const [err, setErr] = useState<string>("");

  async function refresh() {
    const data = await apiGet<CompaniesRes>("/api/companies");
    setCompanies(data.companies);
  }

  useEffect(() => { refresh().catch(e => setErr(String(e))); }, []);

  async function createCompany() {
    setErr("");
    try {
      await apiPostJson("/api/companies", { name });
      setName("");
      await refresh();
    } catch (e) { setErr(String(e)); }
  }

  return (
    <div>
      <h2>1) 会社</h2>
      {err && <p style={{ color: "crimson" }}>{err}</p>}
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
        {companies.map(c => (
          <a key={c} href={`/device?company=${encodeURIComponent(c)}`}
             style={{ padding:"10px 12px", border:"1px solid #444", borderRadius:8, textDecoration:"none", color:"inherit" }}>
            {c}
          </a>
        ))}
      </div>

      <hr style={{ margin: "20px 0" }} />

      <h3>会社追加</h3>
      <div style={{ display: "flex", gap: 8 }}>
        <input value={name} onChange={e => setName(e.target.value)} placeholder="会社名" style={{ padding: 8, width: 320 }} />
        <button onClick={createCompany} disabled={!name.trim()} style={{ padding: "8px 12px" }}>追加</button>
      </div>
    </div>
  );
}
