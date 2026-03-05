"use client";
import { useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import { apiGet } from "@/components/api";
import BackToHome from "@/components/BackToHome";

type DevicesRes = { company: string; devices: { name: string; preset: boolean }[] };

// 機種名の表示名マッピング
const DEVICE_DISPLAY_NAMES: Record<string, string> = {
  mimamori: "みまもり",
  telecom: "テレコム",
};

export default function DevicePage() {
  const searchParams = useSearchParams();
  const [company, setCompany] = useState<string>("");
  const [data, setData] = useState<DevicesRes | null>(null);
  const [err, setErr] = useState("");

  useEffect(() => {
    setCompany(searchParams.get("company") ?? "");
  }, [searchParams]);

  useEffect(() => {
    if (!company) return;
    (async () => {
      try {
        const d = await apiGet<DevicesRes>(`/api/companies/${encodeURIComponent(company)}/devices`);
        setData(d);
      } catch (e) { setErr(String(e)); }
    })();
  }, [company]);

  return (
    <div>
      <h2>2) 機種</h2>
      <p>会社：<b>{company || "—"}</b></p>
      {err && <p style={{ color: "crimson" }}>{err}</p>}
      {data && (
        <div style={{ display: "flex", gap: 10 }}>
          {data.devices.map(d => (
            <a key={d.name}
               href={d.preset ? `/run?company=${encodeURIComponent(company)}&device=${d.name}` : "#"}
               style={{ padding:"10px 12px", border:"1px solid #444", borderRadius:8, textDecoration:"none", color:"inherit", opacity: d.preset ? 1 : 0.5, pointerEvents: d.preset ? "auto" : "none" }}>
              {DEVICE_DISPLAY_NAMES[d.name] || d.name}
            </a>
          ))}
        </div>
      )}
      <BackToHome />
    </div>
  );
}
