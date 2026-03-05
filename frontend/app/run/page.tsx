"use client";
import { use, useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { API_BASE, apiGet } from "@/components/api";
import BackToHome from "@/components/BackToHome";
import { useUploadFilesContext } from "@/components/UploadFilesContext";

const DEVICE_DISPLAY_NAMES: Record<string, string> = {
  mimamori: "みまもり",
  telecom: "テレコム",
};

type JobProgress = {
  jobId: string;
  status: string;
  totalPdfs: number;
  processedPdfs: number;
};

export default function RunPage({
  searchParams,
}: {
  searchParams: Promise<{ company?: string; device?: string }>;
}) {
  const params = use(searchParams);
  const router = useRouter();
  const { lastUploadedFiles, setLastUploadedFiles } = useUploadFilesContext();
  const company = params.company ?? "";
  const device = params.device ?? "";
  const deviceLabel = device ? (DEVICE_DISPLAY_NAMES[device] ?? device) : "";
  const [files, setFiles] = useState<File[]>([]);
  const [taimenFiles, setTaimenFiles] = useState<File[]>([]);
  const [alcoholFiles, setAlcoholFiles] = useState<File[]>([]);
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  const [jobProgress, setJobProgress] = useState<JobProgress | null>(null);

  // 統合確認などから「1つ前の画面に戻る」で戻ってきたときに、保持しておいたファイルを復元する
  useEffect(() => {
    if (!lastUploadedFiles) return;
    setFiles(lastUploadedFiles.files);
    setTaimenFiles(lastUploadedFiles.taimenFiles);
    setAlcoholFiles(lastUploadedFiles.alcoholFiles);
    setLastUploadedFiles(null);
  }, [lastUploadedFiles, setLastUploadedFiles]);

  async function start() {
    setErr("");
    setBusy(true);
    setJobProgress(null);
    try {
      const fd = new FormData();
      fd.append("company", company);
      fd.append("device", device);
      files.forEach(f => fd.append("pdfs", f));
      taimenFiles.forEach(f => fd.append("taimen", f));
      alcoholFiles.forEach(f => fd.append("alcohol", f));
      const res = await fetch(`${API_BASE}/api/jobs`, { method: "POST", body: fd });
      if (!res.ok) throw new Error(await res.text());
      const { jobId } = await res.json();
      setJobProgress({ jobId, status: "queued", totalPdfs: files.length, processedPdfs: 0 });
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    if (!jobProgress?.jobId) return;
    const isRunning = jobProgress.status === "queued" || jobProgress.status === "running";
    if (!isRunning) return;
    const jobId = jobProgress.jobId;
    let t: ReturnType<typeof setTimeout>;
    const tick = async () => {
      try {
        const j = await apiGet<{ jobId: string; status: string; totalPdfs: number; processedPdfs: number }>(`/api/jobs/${jobId}`);
        setJobProgress({
          jobId: j.jobId,
          status: j.status,
          totalPdfs: j.totalPdfs,
          processedPdfs: j.processedPdfs,
        });
        if (j.status === "queued" || j.status === "running") t = setTimeout(tick, 1000);
      } catch (e) {
        setErr(String(e));
      }
    };
    tick();
    return () => { if (t) clearTimeout(t); };
  }, [jobProgress?.jobId, jobProgress?.status]);

  const hasPdfs = files.length > 0;
  const isPolling = jobProgress?.status === "queued" || jobProgress?.status === "running";
  const isDone = jobProgress != null && !isPolling;

  return (
    <div>
      <h2>3) 実行</h2>
      <p>会社：<b>{company}</b> / 機種：<b>{deviceLabel || device}</b></p>
      {err && <p style={{ color: "crimson" }}>{err}</p>}

      {!jobProgress ? (
        <>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 24, marginBottom: 16 }}>
            <div style={{ minWidth: 160 }}>
              <div style={{ fontWeight: 600, marginBottom: 4 }}>{deviceLabel || "みまもり / テレコム"}（デジタコ）</div>
              <input type="file" accept="application/pdf" multiple onChange={(e) => setFiles(Array.from(e.target.files || []))} />
              {files.length > 0 && <span style={{ marginLeft: 8, fontSize: 14 }}>{files.length}件</span>}
            </div>
            <div style={{ minWidth: 160 }}>
              <div style={{ fontWeight: 600, marginBottom: 4 }}>対面</div>
              <input type="file" multiple onChange={(e) => setTaimenFiles(Array.from(e.target.files || []))} />
              {taimenFiles.length > 0 && <span style={{ marginLeft: 8, fontSize: 14 }}>{taimenFiles.length}件</span>}
            </div>
            <div style={{ minWidth: 160 }}>
              <div style={{ fontWeight: 600, marginBottom: 4 }}>アルキラーNEX</div>
              <input type="file" multiple onChange={(e) => setAlcoholFiles(Array.from(e.target.files || []))} />
              {alcoholFiles.length > 0 && <span style={{ marginLeft: 8, fontSize: 14 }}>{alcoholFiles.length}件</span>}
            </div>
          </div>
          <div style={{ marginTop: 10 }}>
            <button onClick={start} disabled={!hasPdfs || busy} style={{ padding: "10px 14px" }}>
              {busy ? "送信中..." : "実行"}
            </button>
          </div>
        </>
      ) : (
        <div style={{ marginTop: 16, maxWidth: 480 }}>
          {isPolling && (
            <>
              <p style={{ marginBottom: 8, fontWeight: 600 }}>処理中…</p>
              <div style={{ height: 24, backgroundColor: "#e0e0e0", borderRadius: 4, overflow: "hidden" }}>
                <div
                  style={{
                    height: "100%",
                    width: `${jobProgress.totalPdfs ? Math.round((100 * jobProgress.processedPdfs) / jobProgress.totalPdfs) : 0}%`,
                    backgroundColor: "#1976d2",
                    transition: "width 0.3s ease",
                  }}
                />
              </div>
              <p style={{ marginTop: 6, fontSize: 14, color: "#555" }}>
                {jobProgress.processedPdfs} / {jobProgress.totalPdfs} PDF 処理済み
              </p>
            </>
          )}
          {isDone && (
            <div style={{ padding: "12px 0" }}>
              <p style={{ marginBottom: 12, fontWeight: 600 }}>処理が完了しました。</p>
              <button
                type="button"
                onClick={() => {
                  setLastUploadedFiles({ files, taimenFiles, alcoholFiles });
                  router.push(`/jobs/${jobProgress.jobId}`);
                }}
                style={{ padding: "10px 16px", border: "1px solid #1976d2", borderRadius: 6, backgroundColor: "#e3f2fd", color: "#1565c0", cursor: "pointer", fontWeight: 600 }}
              >
                結果を確認する →
              </button>
              <button
                type="button"
                onClick={() => setJobProgress(null)}
                style={{ marginLeft: 12, padding: "10px 16px", border: "1px solid #666", borderRadius: 6, background: "#f5f5f5", cursor: "pointer" }}
              >
                別のファイルで再実行
              </button>
            </div>
          )}
        </div>
      )}
      <BackToHome />
    </div>
  );
}
