const getApiBase = () =>
  typeof process !== "undefined" && process.env.NEXT_PUBLIC_API_BASE
    ? process.env.NEXT_PUBLIC_API_BASE
    : "http://localhost:8000";

export const API_BASE = getApiBase();

async function handleResponse(res: Response): Promise<never> {
  const text = await res.text();
  let msg = text;
  try {
    const j = JSON.parse(text) as { detail?: string };
    if (typeof j?.detail === "string") msg = j.detail;
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
