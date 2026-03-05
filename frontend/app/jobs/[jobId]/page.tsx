"use client";
import { use, useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { API_BASE, apiGet, apiPostJson } from "@/components/api";
import BackToHome from "@/components/BackToHome";

type PendingRow = {
  rowIndex: number;
  運行ID: string;
  乗務員ID: string;
  乗務員名: string;
  運行日?: string;
  出庫日時: string;
  帰庫日時: string;
};

type ManualEditFields = {
  出庫日: string;
  出庫時: string;
  出庫分: string;
  帰庫日: string;
  帰庫時: string;
  帰庫分: string;
};

const MANUAL_FIELD_KEYS: (keyof ManualEditFields)[] = ["出庫日", "出庫時", "出庫分", "帰庫日", "帰庫時", "帰庫分"];

function parseDatePart(s: string | undefined): string {
  if (!s?.trim()) return "";
  const match = String(s).trim().match(/^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})/);
  if (!match) return "";
  const [, y, m, d] = match;
  return `${y}/${m.padStart(2, "0")}/${d.padStart(2, "0")}`;
}
/** 日付文字列を YYYY/MM/DD に正規化（比較・キー用） */
function normalizeDateKey(s: string | undefined): string {
  const parsed = parseDatePart(s);
  return parsed || "";
}
/** YYYY/MM/DD を「M月D日」表示用に */
function formatDateLabel(key: string): string {
  if (!key) return "";
  const m = key.match(/^\d{4}\/(\d{1,2})\/(\d{1,2})$/);
  if (!m) return key;
  return `${parseInt(m[1], 10)}月${parseInt(m[2], 10)}日`;
}
function parseTimePart(s: string | undefined): { h: string; mm: string } {
  if (!s?.trim()) return { h: "", mm: "" };
  const m = String(s).trim().match(/(\d{1,2}):(\d{1,2})/);
  if (m) {
    const h = parseInt(m[1], 10);
    return { h: String(Number.isNaN(h) ? "" : h), mm: m[2].padStart(2, "0") };
  }
  const m2 = String(s).trim().match(/\s(\d{1,2})(?::(\d{2}))?$/);
  if (m2) {
    const h = parseInt(m2[1], 10);
    return { h: String(Number.isNaN(h) ? "" : h), mm: (m2[2] ?? "0").padStart(2, "0") };
  }
  return { h: "", mm: "" };
}
function toDatetime(date: string, h: string, mm: string): string {
  const mm2 = String(mm).padStart(2, "0");
  if (!date?.trim() || !h?.trim()) return "";
  return `${date.trim()} ${h.trim()}:${mm2}`;
}

type DriverRow = {
  rowIndex: number;
  運行ID: string;
  乗務員ID: string;
  乗務員名: string;
  出庫日時?: string;
  帰庫日時?: string;
};

type MergeGroupRun = {
  運行ID?: string;
  出庫日時?: string;
  帰庫日時?: string;
  運行日?: string;
};

type MergeGroup = {
  rowIndices: number[];
  運行IDs: string[];
  運行リスト?: MergeGroupRun[];
  乗務員ID: string;
  乗務員名: string;
  運行日リスト?: (string | undefined)[];
};

type LinkRun = {
  rowIndex: number;
  運行ID: string;
  運行日?: string;
  乗務員名?: string;
  出庫日時?: string;
  帰庫日時?: string;
};

type LinkPair = {
  運行ID1: string;
  運行ID2: string;
  運行日を: "first" | "second";
};

/** 2本以上を1つにまとめるグループ。運行日をは採用する運行のインデックス（0-based） */
type LinkGroup = {
  runIds: string[];
  運行日を: number;
};

type AlcoholOnlyCrewRun = { 出庫日時: string; 帰庫日時: string };
type AlcoholOnlyCrewItem = {
  乗務員ID正規化: string;
  乗務員ID?: string;
  乗務員名?: string;
  runs: AlcoholOnlyCrewRun[];
};

type Job = {
  jobId: string;
  company: string;
  device: string;
  status: "queued" | "running" | "succeeded" | "failed" | "manual_input_required" | "merge_decision_required" | "link_decision_required" | "codriver_link_required";
  totalPdfs: number;
  processedPdfs: number;
  errorCount: number;
  warnCount: number;
  artifacts: { excel: boolean; log: boolean };
  pendingRows?: PendingRow[];
  driverRows?: DriverRow[];
  alcoholRunsByCrew?: Record<string, Array<{ 出庫日時: string; 帰庫日時: string }>>;
  mergeGroups?: MergeGroup[];
  mergeSets?: number[][][];
  runDateChoices?: number[];
  linkRuns?: LinkRun[];
  linkPairs?: LinkPair[];
  linkGroups?: LinkGroup[];
  alcoholOnlyCrew?: AlcoholOnlyCrewItem[];
  codriverLinks?: Array<{ 乗務員ID?: string; 乗務員名?: string; 出庫日時?: string; 帰庫日時?: string; driverRowIndex: number }>;
};

function normalizeCrewId(uid: string | undefined): string {
  if (uid == null || String(uid).trim() === "") return "";
  const s = String(uid).trim().replace(/^0+/, "") || "0";
  return s;
}

export default function JobPage({ params }: { params: Promise<{ jobId: string }> }) {
  const { jobId } = use(params);
  const router = useRouter();
  const [job, setJob] = useState<Job | null>(null);
  const [err, setErr] = useState("");
  const [manualEdits, setManualEdits] = useState<Record<number, Partial<ManualEditFields>>>({});
  const manualInputRefs = useRef<Record<string, HTMLInputElement | null>>({});
  /** 3h未満グループごとに「まとめる」運行のインデックス（0-based）。例: [[0,1],[0,1,2]] = G0は運行1・2をまとめる、G1は全部まとめる */
  const [mergeSelections, setMergeSelections] = useState<number[][]>([]);
  const [runDateChoiceIndex, setRunDateChoiceIndex] = useState<Record<number, number>>({});
  const [linkPairs, setLinkPairs] = useState<LinkPair[]>([]);
  const [linkGroups, setLinkGroups] = useState<LinkGroup[]>([]);
  const [linkWantAdd, setLinkWantAdd] = useState(false);
  /** 同乗者紐づけ: key = `${乗務員ID正規化}_${runIndex}`, value = driverRowIndex */
  const [codriverSelections, setCodriverSelections] = useState<Record<string, number>>({});
  const [submitting, setSubmitting] = useState(false);
  const manualSubmitInProgressRef = useRef(false);
  /** 手入力画面: 出庫日で絞り込み（"" = すべて表示、YYYY/MM/DD = その日のみ表示） */
  const [manualDateFilter, setManualDateFilter] = useState<string>("");

  /** 1つ前の画面に戻る（ワークフロー内の前ステップに戻す。merge の前だけブラウザ back） */
  const handleBackToPrevious = async () => {
    setErr("");
    if (job?.status === "merge_decision_required") {
      setMergeSelections([]);
      setRunDateChoiceIndex({});
      router.back();
      return;
    }
    if (job?.status === "link_decision_required" || job?.status === "codriver_link_required" || job?.status === "manual_input_required") {
      setSubmitting(true);
      try {
        await apiPostJson<{ ok: boolean; status: string }>(`/api/jobs/${jobId}/revert-step`, {});
        const j = await apiGet<Job>(`/api/jobs/${jobId}`);
        setJob(j);
        setManualEdits({});
        if (j.status === "merge_decision_required" && j.mergeGroups?.length && j.mergeSets != null && j.runDateChoices != null) {
          const sel: number[][] = j.mergeGroups.map((g, gi) => {
            const rowIndices = g.rowIndices ?? [];
            const sets = j.mergeSets![gi] ?? [];
            const checked: number[] = [];
            sets.forEach((s: number[]) => {
              if (s.length >= 2) s.forEach((idx) => { const ri = rowIndices.indexOf(idx); if (ri >= 0) checked.push(ri); });
            });
            return checked.length ? checked : rowIndices.map((_, ri) => ri);
          });
          setMergeSelections(sel);
          const rdc: Record<number, number> = {};
          (j.runDateChoices ?? []).forEach((v, i) => { rdc[i] = v; });
          setRunDateChoiceIndex(rdc);
        } else {
          setMergeSelections([]);
          setRunDateChoiceIndex({});
        }
        if (j.status === "link_decision_required") {
          setLinkWantAdd(true);
          if ((j.linkGroups?.length ?? 0) > 0) {
            setLinkGroups(j.linkGroups!);
            setLinkPairs([]);
          } else if ((j.linkPairs?.length ?? 0) > 0) {
            setLinkGroups(j.linkPairs!.map((p) => ({
              runIds: [p.運行ID1, p.運行ID2].filter(Boolean),
              運行日を: p.運行日を === "second" ? 1 : 0,
            })));
            setLinkPairs(j.linkPairs!);
          } else {
            setLinkGroups([]);
            setLinkPairs([]);
          }
        } else {
          setLinkPairs([]);
          setLinkGroups([]);
          setLinkWantAdd(false);
        }
        if (j.status === "codriver_link_required" && (j.codriverLinks?.length ?? 0) > 0 && (j.alcoholOnlyCrew?.length ?? 0) > 0) {
          const sel: Record<string, number> = {};
          j.alcoholOnlyCrew!.forEach((c) => {
            (c.runs ?? []).forEach((run, runIndex) => {
              const key = `${c.乗務員ID正規化 ?? ""}_${runIndex}`;
              const link = j.codriverLinks!.find(
                (l) => (normalizeCrewId(l.乗務員ID) === (c.乗務員ID正規化 ?? "") && (l.出庫日時 ?? "") === (run.出庫日時 ?? "") && (l.帰庫日時 ?? "") === (run.帰庫日時 ?? ""))
              );
              if (link != null && link.driverRowIndex >= 0) sel[key] = link.driverRowIndex;
            });
          });
          setCodriverSelections(sel);
        } else {
          setCodriverSelections({});
        }
      } catch (e) {
        setErr(String(e));
      } finally {
        setSubmitting(false);
      }
      return;
    }
    setMergeSelections([]);
    setRunDateChoiceIndex({});
    setLinkPairs([]);
    setLinkWantAdd(false);
    setManualEdits({});
    router.back();
  };

  useEffect(() => {
    let t: any;
    const tick = async () => {
      try {
        const j = await apiGet<Job>(`/api/jobs/${jobId}`);
        setJob(j);
        if (j.status === "queued" || j.status === "running") t = setTimeout(tick, 1000);
        if (j.status === "merge_decision_required" && j.mergeGroups?.length) {
          if (j.mergeSets != null && j.runDateChoices != null) {
            const sel: number[][] = j.mergeGroups.map((g, gi) => {
              const rowIndices = g.rowIndices ?? [];
              const sets = j.mergeSets![gi] ?? [];
              const checked: number[] = [];
              sets.forEach((s: number[]) => {
                if (s.length >= 2) s.forEach((idx) => { const ri = rowIndices.indexOf(idx); if (ri >= 0) checked.push(ri); });
              });
              return checked.length ? checked : rowIndices.map((_, ri) => ri);
            });
            setMergeSelections(sel);
            const rdc: Record<number, number> = {};
            (j.runDateChoices ?? []).forEach((v, i) => { rdc[i] = v; });
            setRunDateChoiceIndex(rdc);
          } else {
            setMergeSelections((prev) =>
              prev.length === j.mergeGroups!.length
                ? prev
                : j.mergeGroups!.map((g) => (g.運行リスト ?? []).map((_, ri) => ri))
            );
          }
        }
        if (j.status === "link_decision_required") {
          setLinkWantAdd(true);
          if ((j.linkGroups?.length ?? 0) > 0) {
            setLinkGroups(j.linkGroups!);
            setLinkPairs([]);
          } else if ((j.linkPairs?.length ?? 0) > 0) {
            setLinkGroups(j.linkPairs!.map((p) => ({
              runIds: [p.運行ID1, p.運行ID2].filter(Boolean),
              運行日を: p.運行日を === "second" ? 1 : 0,
            })));
            setLinkPairs(j.linkPairs!);
          } else {
            setLinkGroups([]);
            setLinkPairs([]);
          }
        }
        if (j.status === "codriver_link_required" && (j.codriverLinks?.length ?? 0) > 0 && (j.alcoholOnlyCrew?.length ?? 0) > 0) {
          const sel: Record<string, number> = {};
          j.alcoholOnlyCrew!.forEach((c) => {
            (c.runs ?? []).forEach((run, runIndex) => {
              const key = `${c.乗務員ID正規化 ?? ""}_${runIndex}`;
              const link = j.codriverLinks!.find(
                (l) => (normalizeCrewId(l.乗務員ID) === (c.乗務員ID正規化 ?? "") && (l.出庫日時 ?? "") === (run.出庫日時 ?? "") && (l.帰庫日時 ?? "") === (run.帰庫日時 ?? ""))
              );
              if (link != null && link.driverRowIndex >= 0) sel[key] = link.driverRowIndex;
            });
          });
          setCodriverSelections(sel);
        }
      } catch (e) { setErr(String(e)); }
    };
    tick();
    return () => t && clearTimeout(t);
  }, [jobId]);


  const dl = (k: "excel" | "log") => `${API_BASE}/api/jobs/${jobId}/download/${k}`;

  const getManualRow = (r: PendingRow): ManualEditFields => {
    const runDate = r.運行日 ? parseDatePart(String(r.運行日)) : "";
    const outDt = r.出庫日時?.trim();
    const inDt = r.帰庫日時?.trim();
    const outTime = parseTimePart(outDt);
    const inTime = parseTimePart(inDt);
    return {
      出庫日: manualEdits[r.rowIndex]?.出庫日 ?? (runDate || parseDatePart(outDt)),
      出庫時: manualEdits[r.rowIndex]?.出庫時 ?? outTime.h,
      出庫分: manualEdits[r.rowIndex]?.出庫分 ?? outTime.mm,
      帰庫日: manualEdits[r.rowIndex]?.帰庫日 ?? (runDate || parseDatePart(inDt)),
      帰庫時: manualEdits[r.rowIndex]?.帰庫時 ?? inTime.h,
      帰庫分: manualEdits[r.rowIndex]?.帰庫分 ?? inTime.mm,
    };
  };
  const allManualFilled =
    job?.pendingRows?.every((r) => {
      const m = getManualRow(r);
      return m.出庫日?.trim() && m.出庫時?.trim() && m.出庫分?.trim() && m.帰庫日?.trim() && m.帰庫時?.trim() && m.帰庫分?.trim();
    }) ?? false;

  const handleMergeSubmit = async () => {
    if (!job?.mergeGroups?.length) return;
    const mergeSets: number[][][] = [];
    const runDateChoices: number[] = job.mergeGroups.map(() => 0);
    for (let gi = 0; gi < job.mergeGroups.length; gi++) {
      const g = job.mergeGroups[gi];
      const rowIndices = g.rowIndices ?? [];
      const selected = mergeSelections[gi] ?? [];
      const checkedRowIndices = selected
        .filter((ri) => ri >= 0 && ri < rowIndices.length)
        .map((ri) => rowIndices[ri])
        .sort((a, b) => a - b);
      const sets: number[][] = [];
      if (checkedRowIndices.length >= 2) {
        sets.push(checkedRowIndices);
        runDateChoices[gi] = Math.min(runDateChoiceIndex[gi] ?? 0, checkedRowIndices.length - 1);
      }
      for (let ri = 0; ri < rowIndices.length; ri++) {
        if (!selected.includes(ri)) sets.push([rowIndices[ri]]);
      }
      if (sets.length === 0) {
        rowIndices.forEach((idx) => sets.push([idx]));
      }
      mergeSets.push(sets);
    }
    setErr("");
    setSubmitting(true);
    try {
      await apiPostJson(`/api/jobs/${jobId}/complete-merge`, {
        mergeSets,
        runDateChoices,
      });
      const j = await apiGet<Job>(`/api/jobs/${jobId}`);
      setJob(j);
      setMergeSelections([]);
      setRunDateChoiceIndex({});
    } catch (e) {
      setErr(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  const handleLinkSkip = async () => {
    setErr("");
    setSubmitting(true);
    try {
      await apiPostJson(`/api/jobs/${jobId}/complete-link-skip`, {});
      const j = await apiGet<Job>(`/api/jobs/${jobId}`);
      setJob(j);
      setLinkPairs([]);
      setLinkWantAdd(false);
    } catch (e) {
      setErr(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  const handleLinkSubmit = async () => {
    setErr("");
    setSubmitting(true);
    try {
      const payload = linkGroups.length > 0
        ? { linkGroups: linkGroups.map((g) => ({ runIds: g.runIds, 運行日を: g.運行日を })) }
        : { pairs: linkPairs };
      await apiPostJson(`/api/jobs/${jobId}/complete-link-pairs`, payload);
      const j = await apiGet<Job>(`/api/jobs/${jobId}`);
      setJob(j);
      setLinkPairs([]);
      setLinkGroups([]);
      setLinkWantAdd(false);
    } catch (e) {
      setErr(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  const handleCodriverSkip = async () => {
    setErr("");
    setSubmitting(true);
    try {
      await apiPostJson(`/api/jobs/${jobId}/complete-codriver-skip`, {});
      const j = await apiGet<Job>(`/api/jobs/${jobId}`);
      setJob(j);
      setCodriverSelections({});
    } catch (e) {
      setErr(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  const handleCodriverLinkSubmit = async () => {
    const crew = job?.alcoholOnlyCrew ?? [];
    const dr = job?.driverRows ?? [];
    const links: { 乗務員ID正規化: string; runIndex: number; driverRowIndex: number }[] = [];
    for (const c of crew) {
      const norm = c.乗務員ID正規化 ?? "";
      (c.runs ?? []).forEach((_, runIndex) => {
        const key = `${norm}_${runIndex}`;
        const driverRowIndex = codriverSelections[key];
        if (driverRowIndex != null && driverRowIndex >= 0 && dr.some((d) => d.rowIndex === driverRowIndex)) {
          links.push({ 乗務員ID正規化: norm, runIndex, driverRowIndex });
        }
      });
    }
    setErr("");
    setSubmitting(true);
    try {
      await apiPostJson(`/api/jobs/${jobId}/complete-codriver-link`, { links });
      const j = await apiGet<Job>(`/api/jobs/${jobId}`);
      setJob(j);
      setCodriverSelections({});
    } catch (e) {
      setErr(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  const handleManualSubmit = async () => {
    if (submitting || manualSubmitInProgressRef.current || !job?.pendingRows?.length) return;
    manualSubmitInProgressRef.current = true;
    setSubmitting(true);
    setErr("");
    const pendingRows = job.pendingRows;
    for (const r of pendingRows) {
      const m = getManualRow(r);
      const isEmpty =
        !String(m.出庫日 ?? "").trim() &&
        !String(m.出庫時 ?? "").trim() &&
        !String(m.出庫分 ?? "").trim() &&
        !String(m.帰庫日 ?? "").trim() &&
        !String(m.帰庫時 ?? "").trim() &&
        !String(m.帰庫分 ?? "").trim();
      if (isEmpty) continue;
      if (!validateTimeBlock(r.rowIndex, "出庫", m.出庫時, m.出庫分) || !validateTimeBlock(r.rowIndex, "帰庫", m.帰庫時, m.帰庫分)) {
        setErr("正確な時刻を入力してください");
        manualSubmitInProgressRef.current = false;
        setSubmitting(false);
        return;
      }
    }
    try {
      const entries = pendingRows.map((r) => {
        const m = getManualRow(r);
        return {
          rowIndex: r.rowIndex,
          出庫日時: toDatetime(m.出庫日, m.出庫時, m.出庫分),
          帰庫日時: toDatetime(m.帰庫日, m.帰庫時, m.帰庫分),
        };
      });
      await apiPostJson<{ ok?: boolean; status?: string; message?: string }>(`/api/jobs/${jobId}/complete-manual`, { entries });
      // ファイルが書き込まれるまで短く待ってから fetch で取得し、Blob でダウンロード（リンク直接だと準備完了前に GET されることがあるため）
      const excelUrl = dl("excel");
      let res = await fetch(excelUrl, { cache: "no-store" });
      if (!res.ok) {
        await new Promise((r) => setTimeout(r, 400));
        res = await fetch(excelUrl, { cache: "no-store" });
      }
      if (!res.ok) throw new Error("Excel の準備ができていません。しばらく待ってから再度「ダウンロード」を押してください。");
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "output.xlsx";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      const j = await apiGet<Job>(`/api/jobs/${jobId}`);
      setJob(j);
      if (j.status === "succeeded") setManualEdits({});
    } catch (e) {
      setErr(String(e));
    } finally {
      manualSubmitInProgressRef.current = false;
      setSubmitting(false);
    }
  };

  const updateManualField = (rowIndex: number, field: keyof ManualEditFields, value: string) => {
    setManualEdits((prev) => ({
      ...prev,
      [rowIndex]: { ...prev[rowIndex], [field]: value },
    }));
  };

  /** 時(0-23)・分(0-59)の妥当性。空は未入力として true。 */
  const validateTimeBlock = (
    rowIndex: number,
    kind: "出庫" | "帰庫",
    hourValue: string,
    minValue: string
  ): boolean => {
    if (hourValue.trim() !== "") {
      const h = parseInt(hourValue, 10);
      if (Number.isNaN(h) || h < 0 || h >= 24) return false;
    }
    if (minValue.trim() !== "") {
      const m = parseInt(minValue, 10);
      if (Number.isNaN(m) || m < 0 || m >= 60) return false;
    }
    return true;
  };

  const clearTimeBlock = (rowIndex: number, kind: "出庫" | "帰庫") => {
    if (kind === "出庫") {
      setManualEdits((prev) => ({ ...prev, [rowIndex]: { ...prev[rowIndex], 出庫時: "", 出庫分: "" } }));
    } else {
      setManualEdits((prev) => ({ ...prev, [rowIndex]: { ...prev[rowIndex], 帰庫時: "", 帰庫分: "" } }));
    }
  };

  const focusNextEmpty = (currentRowIndex: number, currentField: keyof ManualEditFields) => {
    const rows = job?.pendingRows ?? [];
    const currentIdx = MANUAL_FIELD_KEYS.indexOf(currentField);
    const startRow = rows.findIndex((pr) => pr.rowIndex === currentRowIndex);
    if (startRow < 0) return;
    for (let r = startRow; r < rows.length; r++) {
      const row = rows[r];
      const startCol = r === startRow ? currentIdx + 1 : 0;
      for (let c = startCol; c < MANUAL_FIELD_KEYS.length; c++) {
        const field = MANUAL_FIELD_KEYS[c];
        const m = getManualRow(row);
        if (!String(m[field] ?? "").trim()) {
          const key = `${row.rowIndex}-${field}`;
          manualInputRefs.current[key]?.focus();
          return;
        }
      }
    }
  };

  /** 帰庫分の次: 同じ行の帰庫日はスキップし、次の行の出庫日から空きを探す */
  const focusNextEmptyAfterReturnMinute = (currentRowIndex: number) => {
    const rows = job?.pendingRows ?? [];
    const startRow = rows.findIndex((pr) => pr.rowIndex === currentRowIndex);
    if (startRow < 0) return;
    for (let r = startRow + 1; r < rows.length; r++) {
      const row = rows[r];
      for (let c = 0; c < MANUAL_FIELD_KEYS.length; c++) {
        const field = MANUAL_FIELD_KEYS[c];
        const m = getManualRow(row);
        if (!String(m[field] ?? "").trim()) {
          const key = `${row.rowIndex}-${field}`;
          manualInputRefs.current[key]?.focus();
          return;
        }
      }
    }
    const sameRow = rows[startRow];
    const m = getManualRow(sameRow);
    if (!String(m.帰庫日 ?? "").trim()) {
      manualInputRefs.current[`${currentRowIndex}-帰庫日`]?.focus();
    }
  };

  const handleManualKeyDown = (rowIndex: number, field: keyof ManualEditFields, e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key !== "Enter") return;
    e.preventDefault();
    if (field === "出庫時") {
      const hourVal = (e.target as HTMLInputElement).value;
      const minVal = manualInputRefs.current[`${rowIndex}-出庫分`]?.value ?? "";
      if (!validateTimeBlock(rowIndex, "出庫", hourVal, minVal)) {
        setErr("正確な時刻を入力してください");
        clearTimeBlock(rowIndex, "出庫");
        manualInputRefs.current[`${rowIndex}-出庫時`]?.focus();
        return;
      }
      setErr("");
      manualInputRefs.current[`${rowIndex}-出庫分`]?.focus();
      return;
    }
    if (field === "出庫分") {
      const hourVal = manualInputRefs.current[`${rowIndex}-出庫時`]?.value ?? "";
      const minVal = (e.target as HTMLInputElement).value;
      if (!validateTimeBlock(rowIndex, "出庫", hourVal, minVal)) {
        setErr("正確な時刻を入力してください");
        clearTimeBlock(rowIndex, "出庫");
        manualInputRefs.current[`${rowIndex}-出庫時`]?.focus();
        return;
      }
      setErr("");
      manualInputRefs.current[`${rowIndex}-帰庫時`]?.focus();
      return;
    }
    if (field === "帰庫時") {
      const hourVal = (e.target as HTMLInputElement).value;
      const minVal = manualInputRefs.current[`${rowIndex}-帰庫分`]?.value ?? "";
      if (!validateTimeBlock(rowIndex, "帰庫", hourVal, minVal)) {
        setErr("正確な時刻を入力してください");
        clearTimeBlock(rowIndex, "帰庫");
        manualInputRefs.current[`${rowIndex}-帰庫時`]?.focus();
        return;
      }
      setErr("");
      manualInputRefs.current[`${rowIndex}-帰庫分`]?.focus();
      return;
    }
    if (field === "帰庫分") {
      const hourVal = manualInputRefs.current[`${rowIndex}-帰庫時`]?.value ?? "";
      const minVal = (e.target as HTMLInputElement).value;
      if (!validateTimeBlock(rowIndex, "帰庫", hourVal, minVal)) {
        setErr("正確な時刻を入力してください");
        clearTimeBlock(rowIndex, "帰庫");
        manualInputRefs.current[`${rowIndex}-帰庫時`]?.focus();
        return;
      }
      setErr("");
      focusNextEmptyAfterReturnMinute(rowIndex);
      return;
    }
    if (field === "出庫日" || field === "帰庫日") {
      setErr("");
      focusNextEmpty(rowIndex, field);
    }
  };

  return (
    <div>
      <h2 style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
        4) 結果
        {err && <span style={{ color: "crimson", fontWeight: "normal", fontSize: "0.85em" }}>{err}</span>}
      </h2>
      {!job && <p>読み込み中...</p>}
      {job && (
        <div>
          {job.status === "merge_decision_required" && job.mergeGroups && job.mergeGroups.length > 0 && (
            <div style={{ marginTop: 16, marginBottom: 16 }}>
              <p style={{ marginBottom: 8 }}>
                あなたがアップロードしたデジタコデータが「1日に2つ以上ある」ドライバーリストです。
                <br />
                <br />
                同じ乗務員で「帰庫」→次の「出庫」が3時間未満の運行があります。まとめる運行を個別に選択してください。
              </p>
              <table border={1} cellPadding={8} style={{ borderCollapse: "collapse", tableLayout: "auto", width: "100%", minWidth: 800 }}>
                  <thead>
                    <tr>
                      <th style={{ minWidth: 90, whiteSpace: "nowrap" }}>乗務員ID</th>
                      <th style={{ minWidth: 100, whiteSpace: "nowrap" }}>乗務員名</th>
                      <th style={{ minWidth: 320, whiteSpace: "nowrap" }}>運行ID / 出庫日時 / 帰庫日時</th>
                      <th style={{ minWidth: 280, whiteSpace: "nowrap" }}>まとめる運行</th>
                      <th style={{ minWidth: 200, whiteSpace: "nowrap" }}>運行日をどれにしますか？</th>
                    </tr>
                  </thead>
                  <tbody>
                  {job.mergeGroups.map((g, gi) => {
                    const dates = g.運行日リスト ?? [];
                    const runs = g.運行リスト ?? [];
                    const selected = mergeSelections[gi] ?? [];
                    const mergedCount = selected.length;
                    const choiceIdx = runDateChoiceIndex[gi] ?? 0;
                    const mergedDates = selected.map((ri) => dates[ri]);
                    return (
                      <tr key={gi}>
                        <td style={{ whiteSpace: "nowrap" }}>{g.乗務員ID}</td>
                        <td style={{ whiteSpace: "nowrap" }}>{g.乗務員名}</td>
                        <td style={{ minWidth: 320 }}>
                          {runs.length > 0 ? (
                            <table border={0} cellPadding={2} style={{ borderCollapse: "collapse", width: "100%", minWidth: 300 }}>
                              <tbody>
                                {runs.map((r, ri) => (
                                  <tr key={ri}>
                                    <td style={{ borderBottom: ri < runs.length - 1 ? "1px solid #ddd" : undefined, whiteSpace: "nowrap" }}>
                                      <strong>{r.運行ID ?? "—"}</strong><br />
                                      出庫: {r.出庫日時 ?? "—"} / 帰庫: {r.帰庫日時 ?? "—"}
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          ) : (
                            g.運行IDs.join(", ")
                          )}
                        </td>
                        <td style={{ verticalAlign: "top", minWidth: 280 }}>
                          {runs.length > 0 ? (
                            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                              {runs.map((r, ri) => (
                                <label key={ri} style={{ display: "flex", alignItems: "center", gap: 6, cursor: "pointer" }}>
                                  <input
                                    type="checkbox"
                                    checked={selected.includes(ri)}
                                    onChange={() => {
                                      setMergeSelections((prev) => {
                                        const n = [...(prev[gi] ?? [])];
                                        const idx = n.indexOf(ri);
                                        if (idx >= 0) n.splice(idx, 1);
                                        else n.push(ri);
                                        n.sort((a, b) => a - b);
                                        const out = [...prev];
                                        out[gi] = n;
                                        return out;
                                      });
                                    }}
                                  />
                                  <span>運行{ri + 1}: {r.運行ID ?? "—"}</span>
                                </label>
                              ))}
                            </div>
                          ) : (
                            <span>—</span>
                          )}
                        </td>
                        <td style={{ minWidth: 200 }}>
                          {mergedCount >= 2 && mergedDates.length >= 2 ? (() => {
                            const seen = new Set<string>();
                            const options: { label: string; value: number }[] = [];
                            mergedDates.forEach((d, di) => {
                              const key = d != null ? String(d).trim() : "";
                              if (!seen.has(key)) {
                                seen.add(key);
                                options.push({ label: key || "—", value: di });
                              }
                            });
                            const currentDate = mergedDates[choiceIdx] != null ? String(mergedDates[choiceIdx]).trim() : "";
                            const selectedValue = options.find((o) => (mergedDates[o.value] != null ? String(mergedDates[o.value]).trim() : "") === currentDate)?.value ?? options[0]?.value ?? 0;
                            return (
                              <select
                                value={selectedValue}
                                onChange={(e) => setRunDateChoiceIndex((prev) => ({ ...prev, [gi]: Number(e.target.value) }))}
                                style={{ minWidth: 200 }}
                              >
                                {options.map((o) => (
                                  <option key={o.value} value={o.value}>{o.label}</option>
                                ))}
                              </select>
                            );
                          })() : mergedCount >= 2 && mergedDates.length === 1 ? (
                            <span>{mergedDates[0] != null ? String(mergedDates[0]) : "—"}</span>
                          ) : (
                            <span style={{ color: "#888" }}>2つ以上選択で表示</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
              <div style={{ display: "flex", gap: 8, marginTop: 10, flexWrap: "wrap" }}>
                <button
                  type="button"
                  onClick={handleBackToPrevious}
                  disabled={submitting}
                  style={{ padding: "8px 16px", border: "1px solid #666", background: "#f5f5f5" }}
                >
                  {submitting ? "戻っています..." : "1つ前の画面に戻る"}
                </button>
                <button
                  type="button"
                  onClick={handleMergeSubmit}
                  disabled={submitting}
                  style={{ padding: "8px 16px", border: "1px solid #1976d2", background: "#e3f2fd", color: "#1565c0", fontWeight: 600 }}
                >
                  {submitting ? "処理中..." : "次へ"}
                </button>
              </div>
            </div>
          )}

          {job.status === "link_decision_required" && (
            <div style={{ marginTop: 16, marginBottom: 16 }}>
              <p style={{ marginBottom: 8 }}>
              あなたがアップロードしたデジタコデータが「1日に2つ以上ある」ドライバーリストです。
              <br />
              <br />
              同じ乗務員で「帰庫」→次の「出庫」が3時間以上空いているが、複数運行データを一つの運行に紐づけたいものはありますか？
              </p>
              <div style={{ marginBottom: 12 }}>
                <button
                  type="button"
                  onClick={() => setLinkWantAdd(true)}
                  style={{
                    marginRight: 8,
                    padding: "8px 16px",
                    backgroundColor: linkWantAdd ? "#c8e6c9" : undefined,
                    border: linkWantAdd ? "2px solid #2e7d32" : "1px solid #ccc",
                    fontWeight: linkWantAdd ? "bold" : undefined,
                  }}
                >
                  はい（ペアを指定）
                </button>
                <button
                  type="button"
                  onClick={() => setLinkWantAdd(false)}
                  style={{
                    padding: "8px 16px",
                    backgroundColor: !linkWantAdd ? "#e0e0e0" : undefined,
                    border: !linkWantAdd ? "2px solid #616161" : "1px solid #ccc",
                    fontWeight: !linkWantAdd ? "bold" : undefined,
                  }}
                >
                  いいえ（このまま次へ）
                </button>
              </div>
              {linkWantAdd && job.linkRuns && job.linkRuns.length >= 2 && (
                <div style={{ border: "1px solid #ccc", padding: 12, marginBottom: 12, maxWidth: 900 }}>
                  <p style={{ marginBottom: 8, fontWeight: "bold" }}>紐づける運行グループ（2本以上で1つにまとめられます）</p>
                  <p style={{ marginBottom: 8, fontSize: 14, color: "#555" }}>各グループで運行を複数選び、運行日をどれに合わせるか選んでください。3本・4本まとめも可能です。</p>
                  {linkGroups.map((grp, gi) => (
                    <div key={gi} style={{ border: "1px solid #eee", padding: 10, marginBottom: 10, borderRadius: 6 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap", marginBottom: 6 }}>
                        {grp.runIds.map((rid, ri) => {
                          const r = job.linkRuns!.find((x) => x.運行ID === rid);
                          return (
                            <span key={rid} style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
                              {ri > 0 && <span style={{ color: "#888" }}>＋</span>}
                              <span style={{ fontSize: 13 }}>
                                {r ? `${r.運行ID} / ${r.乗務員名 ?? "—"}` : rid}
                              </span>
                              <button type="button" onClick={() => setLinkGroups((prev) => { const n = [...prev]; n[gi] = { ...n[gi], runIds: n[gi].runIds.filter((id) => id !== rid) }; return n; })} style={{ padding: "2px 6px", fontSize: 12 }}>削除</button>
                            </span>
                          );
                        })}
                        <select
                          value=""
                          onChange={(e) => {
                            const v = e.target.value;
                            if (!v) return;
                            setLinkGroups((prev) => { const n = [...prev]; if (!n[gi].runIds.includes(v)) n[gi] = { ...n[gi], runIds: [...n[gi].runIds, v] }; return n; });
                            e.target.value = "";
                          }}
                          style={{ minWidth: 200, fontSize: 13 }}
                        >
                          <option value="">— 運行を追加 —</option>
                          {job.linkRuns!.filter((r) => !grp.runIds.includes(r.運行ID))
                            .sort((a, b) => {
                              const nameA = (a.乗務員名 ?? "").trim();
                              const nameB = (b.乗務員名 ?? "").trim();
                              if (nameA !== nameB) return nameA.localeCompare(nameB, "ja");
                              return String(a.運行ID ?? "").localeCompare(String(b.運行ID ?? ""), "ja");
                            })
                            .map((r) => (
                            <option key={r.rowIndex} value={r.運行ID}>
                              {r.運行ID} / {r.乗務員名 ?? "—"} 出庫:{r.出庫日時 ?? "—"} 帰庫:{r.帰庫日時 ?? "—"}
                            </option>
                          ))}
                        </select>
                      </div>
                      {grp.runIds.length >= 2 && (() => {
                        const runDates = grp.runIds.map((rid) => {
                          const r = job.linkRuns!.find((x) => x.運行ID === rid);
                          return r?.運行日 != null && String(r.運行日).trim() ? r.運行日 : "";
                        });
                        const seen = new Set<string>();
                        const options: { label: string; value: number }[] = [];
                        runDates.forEach((d, idx) => {
                          if (!seen.has(d)) {
                            seen.add(d);
                            options.push({ label: d || "—", value: idx });
                          }
                        });
                        const currentDate = runDates[grp.運行日を] ?? "";
                        const selectedValue = options.find((o) => runDates[o.value] === currentDate)?.value ?? grp.運行日を;
                        return (
                          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                            <span style={{ fontSize: 13 }}>運行日を</span>
                            <select
                              value={selectedValue}
                              onChange={(e) => setLinkGroups((prev) => { const n = [...prev]; n[gi] = { ...n[gi], 運行日を: Number(e.target.value) }; return n; })}
                              style={{ minWidth: 180 }}
                            >
                              {options.map((o) => (
                                <option key={o.value} value={o.value}>{o.label}</option>
                              ))}
                            </select>
                          </div>
                        );
                      })()}
                      <button type="button" onClick={() => setLinkGroups((prev) => prev.filter((_, i) => i !== gi))} style={{ marginTop: 6, padding: "2px 8px", fontSize: 12 }}>このグループを削除</button>
                    </div>
                  ))}
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 8 }}>
                    <button type="button" onClick={() => setLinkGroups((prev) => [...prev, { runIds: [], 運行日を: 0 }])} style={{ padding: "6px 12px" }}>
                      グループを追加
                    </button>
                    <button type="button" onClick={() => { setLinkWantAdd(false); setLinkGroups([]); setLinkPairs([]); }} style={{ padding: "6px 12px" }}>キャンセル</button>
                  </div>
                </div>
              )}
              {linkWantAdd && (!job.linkRuns || job.linkRuns.length < 2) && (
                <p style={{ color: "#666" }}>紐づけ可能な運行が2件未満です。「いいえ」を選んで「次へ」で進んでください。</p>
              )}
              <div style={{ marginTop: 12, display: "flex", gap: 8, flexWrap: "wrap" }}>
                <button
                  type="button"
                  onClick={handleBackToPrevious}
                  disabled={submitting}
                  style={{ padding: "8px 16px", border: "1px solid #666", background: "#f5f5f5" }}
                >
                  {submitting ? "戻っています..." : "1つ前の画面に戻る"}
                </button>
                <button
                  type="button"
                  onClick={linkWantAdd ? handleLinkSubmit : handleLinkSkip}
                  disabled={submitting || (linkWantAdd && (linkGroups.length === 0 || linkGroups.some((g) => g.runIds.length < 2)))}
                  style={{ padding: "8px 16px", border: "1px solid #1976d2", background: "#e3f2fd", color: "#1565c0", fontWeight: 600 }}
                >
                  {submitting ? "処理中..." : "次へ"}
                </button>
              </div>
            </div>
          )}

          {job.status === "codriver_link_required" && (
            <div style={{ marginTop: 16, marginBottom: 16, position: "relative" }}>
              <p style={{ marginBottom: 12, fontSize: 14, color: "#555" }}>
                以下は「デジタコ」のデータがなく「アルコール」のデータだけある乗務員の一覧です。
                <br />
                <strong style={{ fontSize: 16 }}>① この中から横乗りした運行のみ、同乗者の運行を選択してください。</strong>
                <br />
                <strong style={{ fontSize: 16 }}>② 「関係ない日」や「横乗りしてない」ものは未選択でOKです。</strong>
              </p>
              {job.alcoholOnlyCrew && job.alcoholOnlyCrew.length > 0 && job.driverRows && job.driverRows.length > 0 ? (
                <>
                  <div style={{ position: "absolute", top: -20, right: -120, maxWidth: 760, fontSize: 14, background: "#fff8e1", border: "1px solid #ffb300", padding: "12px 20px", borderRadius: 6, lineHeight: 1.7, boxShadow: "0 3px 8px rgba(0,0,0,0.2)" }}>
                    <div style={{ fontWeight: "bold", marginBottom: 4 }}>コメント</div>
                    <div>Q. 添乗してないのに以下のリストになんで出てくるの？</div>
                    <div>A. 「日報のファイルがアップロードされてなかったり、デジタコのIDが本人の乗務員IDじゃなく走行した時だよ。」</div>
                  </div>
                  <div style={{ border: "1px solid #ccc", padding: 12, maxWidth: 1200, overflowX: "auto", marginTop: 64 }}>
                  <table border={1} cellPadding={6} style={{ borderCollapse: "collapse", width: "100%" }}>
                    <thead>
                      <tr>
                        <th style={{ border: "2px solid #000" }}>乗務員ID</th>
                        <th style={{ border: "2px solid #000" }}>乗務員名</th>
                        <th style={{ border: "2px solid #000" }}>出庫日時</th>
                        <th style={{ border: "2px solid #000" }}>帰庫日時</th>
                        <th style={{ border: "2px solid #000" }}>誰と添乗しましたか？</th>
                      </tr>
                    </thead>
                    <tbody>
                      {job.alcoholOnlyCrew.flatMap((c) =>
                        (c.runs ?? []).map((run, runIndex) => {
                          const runs = c.runs ?? [];
                          const groupRowSpan = Math.max(1, runs.length);
                          const isFirst = runIndex === 0;
                          const isLast = runIndex === groupRowSpan - 1;
                          const borderStyleTopBottom: React.CSSProperties = {};
                          if (isFirst) borderStyleTopBottom.borderTop = "2px solid #000";
                          if (isLast) borderStyleTopBottom.borderBottom = "2px solid #000";

                          const key = `${c.乗務員ID正規化 ?? ""}_${runIndex}`;
                          const sel = codriverSelections[key];
                          return (
                            <tr key={key}>
                              {runIndex === 0 ? (
                                <td
                                  rowSpan={groupRowSpan}
                                  style={{ ...borderStyleTopBottom, borderLeft: "2px solid #000" }}
                                >
                                  {c.乗務員ID ?? "—"}
                                </td>
                              ) : null}
                              {runIndex === 0 ? (
                                <td rowSpan={groupRowSpan} style={borderStyleTopBottom}>
                                  {c.乗務員名 ?? "—"}
                                </td>
                              ) : null}
                              <td style={borderStyleTopBottom}>{run.出庫日時 ?? "—"}</td>
                              <td style={borderStyleTopBottom}>{run.帰庫日時 ?? "—"}</td>
                              <td style={{ ...borderStyleTopBottom, borderRight: "2px solid #000" }}>
                                <select
                                  value={sel ?? ""}
                                  onChange={(e) =>
                                    setCodriverSelections((prev) => ({
                                      ...prev,
                                      [key]: Number(e.target.value),
                                    }))
                                  }
                                  style={{ minWidth: 280 }}
                                >
                                  <option value="">— 選択 —</option>
                                  {job.driverRows!.map((d) => (
                                    <option key={d.rowIndex} value={d.rowIndex}>
                                      {d.運行ID ?? "—"} / {d.乗務員名 ?? "—"} 出庫:{d.出庫日時 ?? "—"} 帰庫:{d.帰庫日時 ?? "—"}
                                    </option>
                                  ))}
                                </select>
                              </td>
                            </tr>
                          );
                        })
                      )}
                    </tbody>
                  </table>
                  <div style={{ display: "flex", gap: 8, marginTop: 12, flexWrap: "wrap" }}>
                    <button type="button" onClick={handleBackToPrevious} disabled={submitting} style={{ padding: "8px 16px", border: "1px solid #666", background: "#f5f5f5" }}>
                      {submitting ? "戻っています..." : "1つ前の画面に戻る"}
                    </button>
                    <button type="button" onClick={handleCodriverLinkSubmit} disabled={submitting} style={{ padding: "8px 16px", border: "1px solid #1976d2", background: "#e3f2fd", color: "#1565c0", fontWeight: 600 }}>
                      {submitting ? "処理中..." : "次へ"}
                    </button>
                  </div>
                  </div>
                </>
              ) : (
                <div>
                  <p style={{ color: "#666", marginBottom: 8 }}>同乗者またはデジタコ運行データがありません。</p>
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                    <button type="button" onClick={handleBackToPrevious} disabled={submitting} style={{ padding: "8px 16px", border: "1px solid #666", background: "#f5f5f5" }}>
                      {submitting ? "戻っています..." : "1つ前の画面に戻る"}
                    </button>
                    <button type="button" onClick={handleCodriverSkip} disabled={submitting} style={{ padding: "8px 16px", border: "1px solid #1976d2", background: "#e3f2fd", color: "#1565c0", fontWeight: 600 }}>
                      {submitting ? "処理中..." : "次へ"}
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}

          {job.status === "manual_input_required" && job.pendingRows && job.pendingRows.length > 0 && (() => {
              const pendingRows = job.pendingRows!;
              const uniqueDateKeys = [...new Set(pendingRows.map((r) => normalizeDateKey(getManualRow(r).出庫日)).filter(Boolean))].sort();
              const displayRows = manualDateFilter === "" ? pendingRows : pendingRows.filter((r) => normalizeDateKey(getManualRow(r).出庫日) === manualDateFilter);
              return (
            <div style={{ marginTop: 16, marginBottom: 16, position: "relative" }}>
              <div style={{ position: "absolute", top: -100, right: -5, maxWidth: 400, fontSize: 14, background: "#fff8e1", border: "1px solid #ffb300", padding: "12px 20px", borderRadius: 6, lineHeight: 1.6, boxShadow: "0 3px 8px rgba(0,0,0,0.2)" }}>
                <div style={{ fontWeight: "bold", marginBottom: 4 }}>コメント</div>
                <div>必要ない運行日は未入力でOK</div>
              </div>
              <p style={{ marginBottom: 8 }}>
              「アルコール検知器」を<strong>吹いてないか</strong>、吹いた時間が「デジタコ」の出庫/帰庫時間と<strong>離れすぎてるか</strong>で「紐づかなかった」乗務員の一覧です。
              <br />
              出庫・帰庫の日時を手入力してください。
              </p>
              <div style={{ marginBottom: 10, display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                <span style={{ fontSize: 14, fontWeight: 500 }}>出庫日で絞り込み:</span>
                <select
                  value={manualDateFilter}
                  onChange={(e) => setManualDateFilter(e.target.value)}
                  style={{ padding: "4px 8px", fontSize: 14, minWidth: 140 }}
                >
                  <option value="">すべて表示</option>
                  {uniqueDateKeys.map((key) => (
                    <option key={key} value={key}>{formatDateLabel(key)}</option>
                  ))}
                </select>
                {manualDateFilter !== "" && (
                  <span style={{ fontSize: 13, color: "#666" }}>（{displayRows.length}件表示 / 全{pendingRows.length}件）</span>
                )}
              </div>
              <table border={1} cellPadding={6} style={{ borderCollapse: "collapse", width: "100%", maxWidth: 1000 }}>
                <thead>
                  <tr>
                    <th>運行ID</th>
                    <th>乗務員ID</th>
                    <th>乗務員名</th>
                    <th>出庫（日付 / h:mm）</th>
                    <th>帰庫（日付 / h:mm）</th>
                  </tr>
                </thead>
                <tbody>
                  {displayRows.map((r) => {
                    const m = getManualRow(r);
                    return (
                      <tr key={r.rowIndex}>
                        <td>{r.運行ID}</td>
                        <td>{r.乗務員ID}</td>
                        <td>{r.乗務員名}</td>
                        <td style={{ whiteSpace: "nowrap" }}>
                          <input
                            ref={(el) => { manualInputRefs.current[`${r.rowIndex}-出庫日`] = el; }}
                            type="text"
                            value={m.出庫日}
                            onChange={(e) => updateManualField(r.rowIndex, "出庫日", e.target.value)}
                            onKeyDown={(e) => handleManualKeyDown(r.rowIndex, "出庫日", e)}
                            style={{ width: 100, marginRight: 4 }}
                            size={10}
                          />
                          <input
                            ref={(el) => { manualInputRefs.current[`${r.rowIndex}-出庫時`] = el; }}
                            type="text"
                            inputMode="numeric"
                            value={m.出庫時}
                            onChange={(e) => updateManualField(r.rowIndex, "出庫時", e.target.value.replace(/\D/g, "").slice(0, 2))}
                            onKeyDown={(e) => handleManualKeyDown(r.rowIndex, "出庫時", e)}
                            style={{ width: 28, textAlign: "center" }}
                            maxLength={2}
                          />
                          <span style={{ margin: "0 2px" }}>:</span>
                          <input
                            ref={(el) => { manualInputRefs.current[`${r.rowIndex}-出庫分`] = el; }}
                            type="text"
                            inputMode="numeric"
                            value={m.出庫分}
                            onChange={(e) => updateManualField(r.rowIndex, "出庫分", e.target.value.replace(/\D/g, "").slice(0, 2))}
                            onKeyDown={(e) => handleManualKeyDown(r.rowIndex, "出庫分", e)}
                            style={{ width: 28, textAlign: "center" }}
                            maxLength={2}
                          />
                        </td>
                        <td style={{ whiteSpace: "nowrap" }}>
                          <input
                            ref={(el) => { manualInputRefs.current[`${r.rowIndex}-帰庫日`] = el; }}
                            type="text"
                            value={m.帰庫日}
                            onChange={(e) => updateManualField(r.rowIndex, "帰庫日", e.target.value)}
                            onKeyDown={(e) => handleManualKeyDown(r.rowIndex, "帰庫日", e)}
                            style={{ width: 100, marginRight: 4 }}
                            size={10}
                          />
                          <input
                            ref={(el) => { manualInputRefs.current[`${r.rowIndex}-帰庫時`] = el; }}
                            type="text"
                            inputMode="numeric"
                            value={m.帰庫時}
                            onChange={(e) => updateManualField(r.rowIndex, "帰庫時", e.target.value.replace(/\D/g, "").slice(0, 2))}
                            onKeyDown={(e) => handleManualKeyDown(r.rowIndex, "帰庫時", e)}
                            style={{ width: 28, textAlign: "center" }}
                            maxLength={2}
                          />
                          <span style={{ margin: "0 2px" }}>:</span>
                          <input
                            ref={(el) => { manualInputRefs.current[`${r.rowIndex}-帰庫分`] = el; }}
                            type="text"
                            inputMode="numeric"
                            value={m.帰庫分}
                            onChange={(e) => updateManualField(r.rowIndex, "帰庫分", e.target.value.replace(/\D/g, "").slice(0, 2))}
                            onKeyDown={(e) => handleManualKeyDown(r.rowIndex, "帰庫分", e)}
                            style={{ width: 28, textAlign: "center" }}
                            maxLength={2}
                          />
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
              <div style={{ display: "flex", gap: 8, marginTop: 10, flexWrap: "wrap" }}>
                <button
                  type="button"
                  onClick={handleBackToPrevious}
                  disabled={submitting}
                  style={{ padding: "8px 16px", border: "1px solid #666", background: "#f5f5f5" }}
                >
                  {submitting ? "戻っています..." : "1つ前の画面に戻る"}
                </button>
                <button
                  type="button"
                  onClick={handleManualSubmit}
                  disabled={submitting}
                  style={{ padding: "8px 16px", border: "1px solid #1976d2", background: "#e3f2fd", color: "#1565c0", fontWeight: 600 }}
                  title="押すとExcelを出力し、ダウンロードできます"
                >
                  {submitting ? "ダウンロード中..." : "ダウンロード"}
                </button>
              </div>
            </div>
              );
            })()}

          {job.status === "succeeded" && (
            <p style={{ marginTop: 8 }}>Excelをダウンロードしました。</p>
          )}
        </div>
      )}
      <BackToHome />
    </div>
  );
}
