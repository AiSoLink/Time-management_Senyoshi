const getApiBase = () =>
  typeof process !== "undefined" && process.env.NEXT_PUBLIC_API_BASE
    ? process.env.NEXT_PUBLIC_API_BASE
    : "http://localhost:8000";

export const API_BASE = getApiBase();

type ApiErrorJson = {
  detail?: unknown;
  userSummary?: string;
  message?: string;
  errorId?: string;
  exceptionType?: string;
  path?: string;
};

/** API が返す JSON から、画面用の短文メッセージを組み立てる */
function formatApiErrorMessage(j: ApiErrorJson): string {
  const lines: string[] = [];
  if (typeof j.userSummary === "string" && j.userSummary.trim()) {
    lines.push(j.userSummary.trim());
  } else if (typeof j.detail === "string" && j.detail.trim()) {
    lines.push(j.detail.trim());
  } else if (j.detail != null) {
    lines.push(typeof j.detail === "string" ? j.detail : JSON.stringify(j.detail));
  }
  if (typeof j.errorId === "string" && j.errorId) {
    lines.push(`参照ID: ${j.errorId}`);
  }
  const hasFriendly = typeof j.userSummary === "string" && j.userSummary.trim();
  if (!hasFriendly && typeof j.message === "string" && j.message.trim()) {
    lines.push(`技術メッセージ: ${j.message.trim()}`);
  }
  return lines.filter(Boolean).join("\n");
}

async function handleResponse(res: Response): Promise<never> {
  const text = await res.text();
  let msg = text;
  try {
    const j = JSON.parse(text) as ApiErrorJson;
    const formatted = formatApiErrorMessage(j);
    if (formatted) msg = formatted;
  } catch {
    /* use text as-is */
  }
  throw new Error(msg || `エラー (${res.status})`);
}

export async function apiGet<T>(path: string): Promise<T> {
  let res: Response;
  try {
    res = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  } catch (e) {
    throw new Error(
      "バックエンドに接続できません。API サーバー（例: localhost:8000）が起動しているか確認してください。"
    );
  }
  if (!res.ok) await handleResponse(res);
  return res.json();
}

export async function apiPostJson<T>(path: string, body: unknown): Promise<T> {
  let res: Response;
  try {
    res = await fetch(`${API_BASE}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (e) {
    throw new Error(
      "バックエンドに接続できません。API サーバー（例: localhost:8000）が起動しているか確認してください。"
    );
  }
  if (!res.ok) await handleResponse(res);
  return res.json();
}
