from __future__ import annotations

import json
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from storage.paths import ensure_dirs, APP_ROOT, COMPANIES_DIR, safe_name, job_input_dir, job_output_dir, job_state_path
from storage.state import JobState, Artifacts, save_state, load_state
from job_runner import run_job
from engine.pipeline import complete_manual_input, apply_merge_decision, apply_alcohol_to_run_states, rows_from_run_states, _merge_runs, _write_excel as write_excel
from engine.alcohol_integration import integrate_alcohol, alcohol_runs_by_crew, alcohol_only_crew_list, _normalize_crew_id as normalize_crew_id
from uuid import uuid4


def _run_date_from_row(r: Dict[str, Any]) -> Any:
    """運行日があれば返す。なければ出庫日時/帰庫日時から日付部分を取得。"""
    v = r.get("運行日")
    if v is not None and str(v).strip():
        return v
    s = str(r.get("出庫日時") or r.get("帰庫日時") or "").strip()
    m = re.match(r"(\d{4}[/-]\d{1,2}[/-]\d{1,2})", s)
    return m.group(1).replace("-", "/") if m else None


def _normalize_run_id(rid: str) -> str:
    """テレコム・みまもりでは run_states に 'ID-xxx' が入るが rows_from_run_states では 'xxx' になる。照合用に正規化する。"""
    s = str(rid or "").strip()
    if s.startswith("ID-"):
        return s[3:]
    return s


def _link_runs_after_merge(
    run_states: List[Dict[str, Any]],
    headers: List[str],
    merge_groups: List[Dict[str, Any]],
    merge_sets: Optional[List[List[List[int]]]],
    run_date_choices: List[Any],
    preset_path: Path,
    device: str,
) -> List[Dict[str, Any]]:
    """3時間未満マージ適用後の運行だけを返す。3時間以上画面の linkRuns 用（マージで消えた運行は出さない）。"""
    if not run_states or not headers or not preset_path.exists():
        return []
    if merge_sets is None:
        link_rows = rows_from_run_states(run_states, headers, preset_path, device)
        return [
            {"rowIndex": i, "運行ID": r.get("運行ID"), "運行日": r.get("運行日"), "乗務員名": r.get("乗務員名"), "出庫日時": r.get("出庫日時") or "", "帰庫日時": r.get("帰庫日時") or ""}
            for i, r in enumerate(link_rows)
        ]
    _, new_rows = apply_merge_decision(
        run_states, headers, merge_groups, [], preset_path, device,
        run_date_choices, merge_sets=merge_sets,
    )
    return [
        {
            "rowIndex": i,
            "運行ID": r.get("運行ID"),
            "運行日": r.get("運行日"),
            "乗務員名": r.get("乗務員名"),
            "出庫日時": r.get("出庫日時") or "",
            "帰庫日時": r.get("帰庫日時") or "",
        }
        for i, r in enumerate(new_rows)
    ]


def _apply_entries_to_run_states(
    run_states: List[Dict[str, Any]],
    entries: List[Dict[str, Any]],
    merge_groups: List[Dict[str, Any]],
    merge_choices: List[bool],
    merge_sets: Optional[List[List[List[int]]]] = None,
) -> None:
    """手入力 entries を run_states に反映する。②で同一運行にしたグループは代表行の値で全行を埋める。"""
    if not entries:
        return
    for e in entries:
        row_index = int(e.get("rowIndex", -1))
        if row_index < 0 or row_index >= len(run_states):
            continue
        out_dt = (e.get("出庫日時") or "").strip() or None
        in_dt = (e.get("帰庫日時") or "").strip() or None
        indices_to_update: List[int] = [row_index]
        if merge_sets is not None:
            for sets in merge_sets:
                for s in sets:
                    if row_index in s:
                        indices_to_update = sorted(s)
                        break
                if len(indices_to_update) > 1:
                    break
        else:
            for gi, g in enumerate(merge_groups):
                if gi >= len(merge_choices) or not merge_choices[gi]:
                    continue
                idx_set = set(g.get("rowIndices") or [])
                if row_index in idx_set:
                    indices_to_update = sorted(idx_set)
                    break
        for i in indices_to_update:
            if i >= len(run_states):
                continue
            rs = run_states[i]
            mh = rs.get("merged_header")
            if mh is not None:
                mh["出庫日時"] = out_dt
                mh["帰庫日時"] = in_dt
            if rs.get("merged_row") is not None:
                rs["merged_row"]["出庫日時"] = out_dt
                rs["merged_row"]["帰庫日時"] = in_dt


def _pending_rows_with_group_collapse(
    new_rows: List[Dict[str, Any]],
    merge_groups: List[Dict[str, Any]],
    merge_choices: List[bool],
    merge_sets: Optional[List[List[List[int]]]] = None,
) -> List[Dict[str, Any]]:
    """出庫・帰庫が未取得の行を列挙する。②で「同一運行」にしたグループは代表1行だけ入れる。"""
    missing = [
        {"rowIndex": i, "運行ID": r.get("運行ID"), "乗務員ID": r.get("乗務員ID"), "乗務員名": r.get("乗務員名"), "運行日": _run_date_from_row(r), "出庫日時": r.get("出庫日時") or "", "帰庫日時": r.get("帰庫日時") or ""}
        for i, r in enumerate(new_rows)
        if not r.get("出庫日時") or not r.get("帰庫日時")
    ]
    if not missing:
        return []
    keep_indices = {m["rowIndex"] for m in missing}
    if merge_sets is not None:
        for sets in merge_sets:
            for s in sets:
                if len(s) < 2:
                    continue
                group_missing = set(s) & keep_indices
                if len(group_missing) <= 1:
                    continue
                rep = min(group_missing)
                for i in group_missing:
                    if i != rep:
                        keep_indices.discard(i)
    else:
        for gi, g in enumerate(merge_groups):
            if gi >= len(merge_choices) or not merge_choices[gi]:
                continue
            indices = set(g.get("rowIndices") or [])
            group_missing = indices & keep_indices
            if len(group_missing) <= 1:
                continue
            rep = min(group_missing)
            for i in group_missing:
                if i != rep:
                    keep_indices.discard(i)
    return [m for m in missing if m["rowIndex"] in keep_indices]


DeviceType = Literal["mimamori", "telecom"]

app = FastAPI(title="pdf2excel-localweb", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ensure_dirs()

@app.get("/api/companies")
def list_companies():
    companies = sorted([p.name for p in COMPANIES_DIR.iterdir() if p.is_dir()])
    return {"companies": companies}

@app.post("/api/companies")
def create_company(payload: dict):
    name = safe_name(payload.get("name", ""))
    if not name:
        raise HTTPException(status_code=400, detail="Company name is required.")
    company_dir = COMPANIES_DIR / name
    company_dir.mkdir(parents=True, exist_ok=True)

    sample = COMPANIES_DIR / "__SAMPLE_COMPANY__"
    for dev in ("mimamori", "telecom"):
        target = company_dir / f"{dev}.json"
        if not target.exists():
            if (sample / f"{dev}.json").exists():
                shutil.copy(sample / f"{dev}.json", target)
            else:
                target.write_text('{"meta":{"device_type":"%s","version":"1.0"},"header_extract":{}}' % dev, encoding="utf-8")
    return {"name": name, "created": True}

@app.get("/api/companies/{company}/devices")
def company_devices(company: str):
    company = safe_name(company)
    company_dir = COMPANIES_DIR / company
    if not company_dir.exists():
        raise HTTPException(status_code=404, detail="Company not found.")
    devices = []
    for dev in ("mimamori", "telecom"):
        devices.append({"name": dev, "preset": (company_dir / f"{dev}.json").exists()})
    return {"company": company, "devices": devices}

@app.post("/api/jobs")
async def create_job(
    background: BackgroundTasks,
    company: str = Form(...),
    device: DeviceType = Form(...),
    pdfs: List[UploadFile] = File(...),
    taimen: Optional[List[UploadFile]] = File(None),
    alcohol: Optional[List[UploadFile]] = File(None),
):
    company = safe_name(company)
    company_dir = COMPANIES_DIR / company
    if not company_dir.exists():
        raise HTTPException(status_code=400, detail="Company does not exist.")
    preset = company_dir / f"{device}.json"
    if not preset.exists():
        raise HTTPException(status_code=400, detail="Preset JSON not found for this device.")
    if not pdfs:
        raise HTTPException(status_code=400, detail="No PDFs uploaded.")

    job_id = time.strftime("%Y%m%d_%H%M%S_") + uuid4().hex[:6]
    inp = job_input_dir(job_id)
    out = job_output_dir(job_id)
    inp.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)

    for f in pdfs:
        data = await f.read()
        name = safe_name(Path(f.filename).name)
        if not name.lower().endswith(".pdf"):
            name += ".pdf"
        (inp / name).write_bytes(data)

    def _input_file_name(original_name: str) -> str:
        p = Path(original_name)
        stem = safe_name(p.stem)
        ext = (p.suffix or "").lower()
        if ext in (".csv", ".xlsx"):
            return stem + ext
        return stem + ext if ext else stem

    (inp / "taimen").mkdir(exist_ok=True)
    for f in taimen or []:
        data = await f.read()
        name = _input_file_name(f.filename or "")
        (inp / "taimen" / name).write_bytes(data)

    (inp / "alcohol").mkdir(exist_ok=True)
    for f in alcohol or []:
        data = await f.read()
        name = _input_file_name(f.filename or "")
        (inp / "alcohol" / name).write_bytes(data)

    state = JobState(
        jobId=job_id,
        company=company,
        device=device,
        status="queued",
        totalPdfs=len(pdfs),
        processedPdfs=0,
        errorCount=0,
        warnCount=0,
        startedAt=None,
        finishedAt=None,
        artifacts=Artifacts(excel=False, log=False, skipped=False),
    )
    save_state(job_state_path(job_id), state)

    background.add_task(run_job, job_id)
    return JSONResponse(status_code=202, content={"jobId": job_id})

@app.get("/api/jobs/{jobId}")
def get_job(jobId: str):
    sp = job_state_path(jobId)
    if not sp.exists():
        raise HTTPException(status_code=404, detail="Job not found.")
    state = load_state(sp)
    out: Dict[str, Any] = {**state.__dict__, "artifacts": state.artifacts.__dict__}
    if state.pendingRows is not None:
        out["pendingRows"] = state.pendingRows
    out_dir = job_output_dir(jobId)
    manual_path = out_dir / "manual_input_state.json"
    if manual_path.exists():
        data = json.loads(manual_path.read_text(encoding="utf-8"))
        if state.status == "merge_decision_required":
            out["mergeGroups"] = data.get("mergeGroups") or []
            # 1つ前に戻ったときに入力内容を復元するため
            if data.get("mergeSets") is not None:
                out["mergeSets"] = data["mergeSets"]
            if data.get("runDateChoices") is not None:
                out["runDateChoices"] = data["runDateChoices"]
        if state.status == "link_decision_required":
            run_states = data.get("run_states") or []
            headers = data.get("headers") or []
            merge_groups = data.get("mergeGroups") or []
            merge_sets = data.get("mergeSets")
            run_date_choices = data.get("runDateChoices") or []
            preset_path = COMPANIES_DIR / state.company / f"{state.device}.json"
            if run_states and headers:
                # 3時間未満マージ適用後の運行だけ表示（マージで消えた運行は出さない＝ここで選んでもエラーにしない）
                link_runs = _link_runs_after_merge(
                    run_states, headers, merge_groups, merge_sets, run_date_choices, preset_path, state.device
                )
                out["linkRuns"] = link_runs
            else:
                out["linkRuns"] = data.get("linkRuns") or []
            out["linkPairs"] = data.get("linkPairs") or []
            if data.get("linkGroups") is not None:
                out["linkGroups"] = data["linkGroups"]
        if state.status == "codriver_link_required":
            out["alcoholOnlyCrew"] = data.get("alcoholOnlyCrew") or []
            out["driverRows"] = data.get("driverRows") or []
            out["codriverLinks"] = data.get("codriverLinks") or []
        if state.status == "manual_input_required":
            out["driverRows"] = data.get("driverRows") or []
            out["alcoholRunsByCrew"] = data.get("alcoholRunsByCrew") or {}
    return out

@app.post("/api/jobs/{jobId}/complete-merge")
def complete_merge(jobId: str, body: Dict[str, Any] = Body(...)):
    """②-1: 3h未満グループの「同一運行とするか」を受け取り、合算はせず②-2（link）へ進む。"""
    sp = job_state_path(jobId)
    if not sp.exists():
        raise HTTPException(status_code=404, detail="Job not found.")
    state = load_state(sp)
    if state.status != "merge_decision_required":
        raise HTTPException(status_code=400, detail="このジョブは統合確認待ちではありません。")
    out_dir = job_output_dir(jobId)
    manual_path = out_dir / "manual_input_state.json"
    if not manual_path.exists():
        raise HTTPException(status_code=404, detail="手入力状態が見つかりません。")
    data = json.loads(manual_path.read_text(encoding="utf-8"))
    run_states = data.get("run_states") or []
    headers = data.get("headers") or []
    merge_groups = data.get("mergeGroups") or []
    if not run_states or not headers:
        raise HTTPException(status_code=400, detail="手入力状態が不正です。")
    preset_path = COMPANIES_DIR / state.company / f"{state.device}.json"
    if not preset_path.exists():
        raise HTTPException(status_code=400, detail="プリセットが見つかりません。")
    merge_sets = body.get("mergeSets")
    run_date_choices = body.get("runDateChoices") or []
    if merge_sets is None:
        # 旧形式: mergeChoices を mergeSets に変換
        merge_choices = body.get("mergeChoices") or []
        if len(merge_choices) < len(merge_groups):
            merge_choices = merge_choices + [False] * (len(merge_groups) - len(merge_choices))
        if len(run_date_choices) < len(merge_groups):
            run_date_choices = run_date_choices + [0] * (len(merge_groups) - len(run_date_choices))
        merge_sets = []
        for gi, g in enumerate(merge_groups):
            indices = g.get("rowIndices") or []
            if gi < len(merge_choices) and merge_choices[gi] and len(indices) >= 2:
                merge_sets.append([indices])
            else:
                merge_sets.append([[i] for i in indices])
    if len(run_date_choices) < len(merge_groups):
        run_date_choices = run_date_choices + [0] * (len(merge_groups) - len(run_date_choices))
    # ②では合算しない。選択だけ保存し、②-2（link）画面へ。3時間以上画面にはマージ後の運行だけ出す（ここで消えた運行を選ばせない）
    link_runs = _link_runs_after_merge(
        run_states, headers, merge_groups, merge_sets, run_date_choices, preset_path, state.device
    )
    manual_data: Dict[str, Any] = {
        "run_states": run_states,
        "headers": headers,
        "mergeGroups": merge_groups,
        "mergeSets": merge_sets[: len(merge_groups)],
        "runDateChoices": run_date_choices[: len(merge_groups)],
        "linkRuns": link_runs,
        "previousStep": "merge_decision_required",
    }
    manual_path.write_text(json.dumps(manual_data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    state.status = "link_decision_required"
    state.pendingRows = None
    state.artifacts = Artifacts(excel=False, log=True, skipped=True)
    save_state(sp, state)
    return {"ok": True, "status": "link_decision_required", "message": "3時間以上空いている運行の紐づけを行いますか？"}


@app.post("/api/jobs/{jobId}/revert-step")
def revert_step(jobId: str):
    """ワークフロー内で1つ前の画面に戻る。codriver→link, link→merge, manual_input→previousStep。"""
    sp = job_state_path(jobId)
    if not sp.exists():
        raise HTTPException(status_code=404, detail="Job not found.")
    state = load_state(sp)
    prev: Optional[str] = None
    if state.status == "codriver_link_required":
        prev = "link_decision_required"
    elif state.status == "link_decision_required":
        prev = "merge_decision_required"
    elif state.status == "manual_input_required":
        out_dir = job_output_dir(jobId)
        manual_path = out_dir / "manual_input_state.json"
        if manual_path.exists():
            data = json.loads(manual_path.read_text(encoding="utf-8"))
            prev = data.get("previousStep") or "link_decision_required"
        else:
            prev = "link_decision_required"
    if prev is None:
        raise HTTPException(status_code=400, detail="この画面からは戻れません。")
    state.status = prev
    state.pendingRows = None
    state.artifacts = Artifacts(excel=False, log=True, skipped=True)
    # 同乗者画面に戻る場合、manual_data に alcoholOnlyCrew と driverRows が無いため再計算して書き戻す
    if prev == "codriver_link_required":
        out_dir = job_output_dir(jobId)
        manual_path = out_dir / "manual_input_state.json"
        if manual_path.exists():
            data = json.loads(manual_path.read_text(encoding="utf-8"))
            run_states = data.get("run_states") or []
            inp_dir = job_input_dir(jobId)
            alcohol_events = integrate_alcohol(inp_dir / "taimen", inp_dir / "alcohol")
            if run_states and alcohol_events:
                crew_in_digitaco = _crew_ids_in_run_states(run_states)
                alcohol_only = alcohol_only_crew_list(alcohol_events, crew_in_digitaco)
                driver_rows = [
                    {"rowIndex": i, "運行ID": (rs.get("merged_header") or {}).get("運行ID"), "乗務員ID": (rs.get("merged_header") or {}).get("乗務員ID"), "乗務員名": (rs.get("merged_header") or {}).get("乗務員名"), "出庫日時": (rs.get("merged_header") or {}).get("出庫日時") or "", "帰庫日時": (rs.get("merged_header") or {}).get("帰庫日時") or ""}
                    for i, rs in enumerate(run_states)
                ]
                data["alcoholOnlyCrew"] = alcohol_only
                data["driverRows"] = driver_rows
                manual_path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    save_state(sp, state)
    return {"ok": True, "status": prev}


def _after_link_decision(
    jobId: str,
    sp: Path,
    state: JobState,
    run_states: List[Dict[str, Any]],
    headers: List[str],
    new_rows: List[Dict[str, Any]],
    merge_groups: List[Dict[str, Any]],
    merge_choices: List[bool],
    run_date_choices: List[Any],
    link_pairs: List[Dict[str, Any]],
    codriver_links: Optional[List[Dict[str, Any]]] = None,
    merge_sets: Optional[List[List[List[int]]]] = None,
    came_from: Optional[str] = None,
    link_groups: Optional[List[Dict[str, Any]]] = None,
    link_runs: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """②-2 後: アルコール突合済み・合算前。出庫・帰庫未取得があれば B リストで手入力へ、なければ ⑤ 実行して Excel。came_from は手入力画面から「1つ前」で戻る先。link_runs はデジタコの出庫・帰庫で組んだリスト（戻ったときに再表示するため保持）。"""
    out_dir = job_output_dir(jobId)
    manual_path = out_dir / "manual_input_state.json"
    pending_rows = _pending_rows_with_group_collapse(new_rows, merge_groups, merge_choices, merge_sets)
    codriver_links = codriver_links or []
    if pending_rows:
        inp_dir = job_input_dir(jobId)
        alcohol_events = integrate_alcohol(inp_dir / "taimen", inp_dir / "alcohol")
        alc_runs = alcohol_runs_by_crew(alcohol_events) if alcohol_events else {}
        pending_indices = {p["rowIndex"] for p in pending_rows}
        driver_rows = [
            {"rowIndex": i, "運行ID": (rs.get("merged_header") or {}).get("運行ID"), "乗務員ID": (rs.get("merged_header") or {}).get("乗務員ID"), "乗務員名": (rs.get("merged_header") or {}).get("乗務員名"), "出庫日時": (rs.get("merged_header") or {}).get("出庫日時") or "", "帰庫日時": (rs.get("merged_header") or {}).get("帰庫日時") or ""}
            for i, rs in enumerate(run_states)
            if i not in pending_indices
        ]
        manual_data: Dict[str, Any] = {
            "run_states": run_states,
            "headers": headers,
            "mergeGroups": merge_groups,
            "runDateChoices": run_date_choices,
            "linkPairs": link_pairs,
            "codriverLinks": codriver_links,
            "driverRows": driver_rows,
            "alcoholRunsByCrew": alc_runs,
            "previousStep": came_from or "link_decision_required",
        }
        if link_runs is not None:
            manual_data["linkRuns"] = link_runs
        if link_groups is not None:
            manual_data["linkGroups"] = link_groups
        if merge_sets is not None:
            manual_data["mergeSets"] = merge_sets
        else:
            manual_data["mergeChoices"] = merge_choices
        manual_path.write_text(json.dumps(manual_data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        state.status = "manual_input_required"
        state.pendingRows = pending_rows
        state.artifacts = Artifacts(excel=False, log=True, skipped=True)
        save_state(sp, state)
        return {"ok": True, "message": "出庫・帰庫が未取得の行があるため、手入力をお願いします。"}
    return _do_merge_and_excel(jobId, sp, state, run_states, headers, merge_groups, merge_choices, run_date_choices, link_pairs, codriver_links, merge_sets, link_groups=link_groups)


def _do_merge_and_excel(
    jobId: str,
    sp: Path,
    state: JobState,
    run_states: List[Dict[str, Any]],
    headers: List[str],
    merge_groups: List[Dict[str, Any]],
    merge_choices: List[bool],
    run_date_choices: List[Any],
    link_pairs: List[Dict[str, Any]],
    codriver_links: Optional[List[Dict[str, Any]]] = None,
    merge_sets: Optional[List[List[List[int]]]] = None,
    link_groups: Optional[List[Dict[str, Any]]] = None,
    from_complete_manual: bool = False,
) -> Dict[str, Any]:
    """⑤: 3h未満グループ合算 → 3h以上ペア/グループ合算 → 未取得があれば手入力、なければ Excel 出力。同乗者行を末尾に追加。from_complete_manual のときは未入力行があっても再手入力に戻さず Excel 出力する。"""
    out_dir = job_output_dir(jobId)
    manual_path = out_dir / "manual_input_state.json"
    preset_path = COMPANIES_DIR / state.company / f"{state.device}.json"
    codriver_links = codriver_links or []
    if not preset_path.exists():
        raise HTTPException(status_code=400, detail="プリセットが見つかりません。")
    run_states, new_rows = apply_merge_decision(
        run_states, headers, merge_groups, merge_choices, preset_path, state.device, run_date_choices, merge_sets=merge_sets
    )
    # link_groups: 各要素は { "runIds": [id1, id2, ...], "運行日を": 0 }（運行日をは採用する運行のインデックス 0-based）
    if link_groups is None:
        link_groups = []
        for pair in link_pairs:
            id1 = str(pair.get("運行ID1") or "").strip()
            id2 = str(pair.get("運行ID2") or "").strip()
            if id1 and id2 and id1 != id2:
                use_first = (str(pair.get("運行日を") or "first").strip().lower() in ("first", "1", "1本目"))
                link_groups.append({"runIds": [id1, id2], "運行日を": 0 if use_first else 1})
    run_id_to_index: Dict[str, int] = {str(r.get("運行ID") or ""): i for i, r in enumerate(new_rows)}
    for grp in link_groups:
        run_ids = [str(rid or "").strip() for rid in grp.get("runIds") or [] if str(rid or "").strip()]
        run_ids = list(dict.fromkeys(run_ids))
        if len(run_ids) < 2:
            continue
        indices = []
        for rid in run_ids:
            rid_norm = _normalize_run_id(rid)
            if rid_norm in run_id_to_index:
                indices.append(run_id_to_index[rid_norm])
        indices = sorted(set(indices))
        if len(indices) < 2:
            continue
        rows_grp = [new_rows[i] for i in indices]
        states_grp = [run_states[i] for i in indices]
        order = sorted(range(len(rows_grp)), key=lambda k: (rows_grp[k].get("出庫日時") or "") or "0")
        rows_grp = [rows_grp[o] for o in order]
        states_grp = [states_grp[o] for o in order]
        merged_row, merged_rs = _merge_runs(rows_grp, states_grp, headers)
        date_idx = min(int(grp.get("運行日を") or 0), len(rows_grp) - 1)
        if date_idx >= 0 and rows_grp[date_idx].get("運行日") is not None:
            merged_row["運行日"] = rows_grp[date_idx].get("運行日")
            merged_rs["merged_row"]["運行日"] = merged_row["運行日"]
            merged_rs["merged_header"]["運行日"] = merged_row["運行日"]
        i0, i_last = indices[0], indices[-1]
        run_states = run_states[:i0] + [merged_rs] + run_states[i0 + 1 : i_last] + run_states[i_last + 1 :]
        new_rows = new_rows[:i0] + [merged_row] + new_rows[i0 + 1 : i_last] + new_rows[i_last + 1 :]
        run_id_to_index = {str(r.get("運行ID") or ""): idx for idx, r in enumerate(new_rows)}
    missing = [
        {"rowIndex": i, "運行ID": r.get("運行ID"), "乗務員ID": r.get("乗務員ID"), "乗務員名": r.get("乗務員名"), "運行日": _run_date_from_row(r), "出庫日時": r.get("出庫日時") or "", "帰庫日時": r.get("帰庫日時") or ""}
        for i, r in enumerate(new_rows)
        if not r.get("出庫日時") or not r.get("帰庫日時")
    ]
    # 手入力画面の「入力完了・計算実行」から来た場合は未入力行があっても再手入力に戻さず、そのまま Excel 出力して succeeded にする
    if missing and not from_complete_manual:
        inp_dir = job_input_dir(jobId)
        alcohol_events = integrate_alcohol(inp_dir / "taimen", inp_dir / "alcohol")
        alc_runs = alcohol_runs_by_crew(alcohol_events) if alcohol_events else {}
        pending_indices = {p["rowIndex"] for p in missing}
        driver_rows = [
            {"rowIndex": i, "運行ID": (rs.get("merged_header") or {}).get("運行ID"), "乗務員ID": (rs.get("merged_header") or {}).get("乗務員ID"), "乗務員名": (rs.get("merged_header") or {}).get("乗務員名"), "出庫日時": (rs.get("merged_header") or {}).get("出庫日時") or "", "帰庫日時": (rs.get("merged_header") or {}).get("帰庫日時") or ""}
            for i, rs in enumerate(run_states)
            if i not in pending_indices
        ]
        manual_data = {
            "run_states": run_states,
            "headers": headers,
            "codriverLinks": codriver_links,
            "driverRows": driver_rows,
            "alcoholRunsByCrew": alc_runs,
        }
        manual_path.write_text(json.dumps(manual_data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        state.status = "manual_input_required"
        state.pendingRows = missing
        state.artifacts = Artifacts(excel=False, log=True, skipped=True)
        save_state(sp, state)
        return {"ok": True, "message": "出庫・帰庫が未取得の行があるため、手入力をお願いします。"}
    final_rows = new_rows + _build_codriver_rows(new_rows, codriver_links)
    write_excel(headers, final_rows, out_dir / "output.xlsx")
    state.status = "succeeded"
    state.pendingRows = None
    state.artifacts = Artifacts(excel=True, log=True, skipped=True)
    save_state(sp, state)
    return {"ok": True, "status": "succeeded", "message": "Excel を出力しました。"}


def _build_codriver_rows(base_rows: List[Dict[str, Any]], codriver_links: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """同乗者リンクから Excel 用の行を生成。base_rows[driverRowIndex] をコピーし乗務員・出庫帰庫を差し替え。"""
    out: List[Dict[str, Any]] = []
    for link in codriver_links:
        idx = int(link.get("driverRowIndex", -1))
        if idx < 0 or idx >= len(base_rows):
            continue
        row = dict(base_rows[idx])
        row["乗務員ID"] = link.get("乗務員ID")
        row["乗務員名"] = link.get("乗務員名")
        row["出庫日時"] = link.get("出庫日時") or ""
        row["帰庫日時"] = link.get("帰庫日時") or ""
        out.append(row)
    return out


def _crew_ids_in_run_states(run_states: List[Dict[str, Any]]) -> set:
    """run_states に登場する乗務員ID（正規化）の集合。"""
    out = set()
    for rs in run_states:
        uid = (rs.get("merged_header") or {}).get("乗務員ID")
        out.add(normalize_crew_id(uid))
    return out


@app.post("/api/jobs/{jobId}/complete-link-skip")
def complete_link_skip(jobId: str):
    """②-2 紐づけしないで次へ。アルコールのみの乗務員がいれば同乗者紐づけへ、否则 アルコール突合 → B リスト → 手入力 or ⑤。"""
    sp = job_state_path(jobId)
    if not sp.exists():
        raise HTTPException(status_code=404, detail="Job not found.")
    state = load_state(sp)
    if state.status != "link_decision_required":
        raise HTTPException(status_code=400, detail="このジョブは紐づけ確認待ちではありません。")
    out_dir = job_output_dir(jobId)
    manual_path = out_dir / "manual_input_state.json"
    if not manual_path.exists():
        raise HTTPException(status_code=404, detail="手入力状態が見つかりません。")
    data = json.loads(manual_path.read_text(encoding="utf-8"))
    run_states = data.get("run_states") or []
    headers = data.get("headers") or []
    merge_groups = data.get("mergeGroups") or []
    merge_choices = data.get("mergeChoices") or []
    merge_sets = data.get("mergeSets")
    run_date_choices = data.get("runDateChoices") or []
    if not run_states or not headers:
        raise HTTPException(status_code=400, detail="手入力状態が不正です。")
    preset_path = COMPANIES_DIR / state.company / f"{state.device}.json"
    if not preset_path.exists():
        raise HTTPException(status_code=400, detail="プリセットが見つかりません。")
    inp_dir = job_input_dir(jobId)
    alcohol_events = integrate_alcohol(inp_dir / "taimen", inp_dir / "alcohol")
    if alcohol_events:
        crew_in_digitaco = _crew_ids_in_run_states(run_states)
        alcohol_only = alcohol_only_crew_list(alcohol_events, crew_in_digitaco)
        if alcohol_only:
            driver_rows = [
                {"rowIndex": i, "運行ID": (rs.get("merged_header") or {}).get("運行ID"), "乗務員ID": (rs.get("merged_header") or {}).get("乗務員ID"), "乗務員名": (rs.get("merged_header") or {}).get("乗務員名"), "出庫日時": (rs.get("merged_header") or {}).get("出庫日時") or "", "帰庫日時": (rs.get("merged_header") or {}).get("帰庫日時") or ""}
                for i, rs in enumerate(run_states)
            ]
            manual_data = {"run_states": run_states, "headers": headers, "mergeGroups": merge_groups, "runDateChoices": run_date_choices, "linkPairs": [], "alcoholOnlyCrew": alcohol_only, "driverRows": driver_rows}
            if data.get("linkRuns") is not None:
                manual_data["linkRuns"] = data["linkRuns"]
            if merge_sets is not None:
                manual_data["mergeSets"] = merge_sets
            else:
                manual_data["mergeChoices"] = merge_choices
            manual_path.write_text(json.dumps(manual_data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            state.status = "codriver_link_required"
            state.pendingRows = None
            state.artifacts = Artifacts(excel=False, log=True, skipped=True)
            save_state(sp, state)
            return {"ok": True, "status": "codriver_link_required", "message": "アルコールのデータのみの乗務員がいます。同乗者としてどの運行に紐づけますか？"}
    if alcohol_events:
        apply_alcohol_to_run_states(run_states, alcohol_events, margin_minutes=120)
    new_rows = rows_from_run_states(run_states, headers, preset_path, state.device)
    return _after_link_decision(
        jobId, sp, state, run_states, headers, new_rows,
        merge_groups, merge_choices, run_date_choices, [], merge_sets=merge_sets, came_from="link_decision_required",
        link_runs=data.get("linkRuns"),
    )


@app.post("/api/jobs/{jobId}/complete-link-pairs")
def complete_link_pairs(jobId: str, body: Dict[str, Any] = Body(...)):
    """②-2 ペア指定で次へ。合算はせず保存。アルコール突合 → B リスト（グループ1行化）→ 未取得があれば手入力、なければ ⑤ で Excel。"""
    sp = job_state_path(jobId)
    if not sp.exists():
        raise HTTPException(status_code=404, detail="Job not found.")
    state = load_state(sp)
    if state.status != "link_decision_required":
        raise HTTPException(status_code=400, detail="このジョブは紐づけ確認待ちではありません。")
    out_dir = job_output_dir(jobId)
    manual_path = out_dir / "manual_input_state.json"
    if not manual_path.exists():
        raise HTTPException(status_code=404, detail="手入力状態が見つかりません。")
    data = json.loads(manual_path.read_text(encoding="utf-8"))
    run_states = data.get("run_states") or []
    headers = data.get("headers") or []
    merge_groups = data.get("mergeGroups") or []
    merge_choices = data.get("mergeChoices") or []
    merge_sets = data.get("mergeSets")
    run_date_choices = data.get("runDateChoices") or []
    if not run_states or not headers:
        raise HTTPException(status_code=400, detail="手入力状態が不正です。")
    preset_path = COMPANIES_DIR / state.company / f"{state.device}.json"
    if not preset_path.exists():
        raise HTTPException(status_code=400, detail="プリセットが見つかりません。")
    temp_rows = rows_from_run_states(run_states, headers, preset_path, state.device)
    run_id_to_index: Dict[str, int] = {str(r.get("運行ID") or ""): i for i, r in enumerate(temp_rows)}
    link_groups_arg: Optional[List[Dict[str, Any]]] = body.get("linkGroups")
    pairs: List[Dict[str, Any]] = []
    if link_groups_arg is not None:
        for grp in link_groups_arg:
            run_ids = [str(rid or "").strip() for rid in (grp.get("runIds") or []) if str(rid or "").strip()]
            run_ids = list(dict.fromkeys(run_ids))
            if len(run_ids) < 2:
                raise HTTPException(status_code=400, detail="各グループは2本以上の運行を指定してください。")
            for rid in run_ids:
                if _normalize_run_id(rid) not in run_id_to_index:
                    raise HTTPException(status_code=400, detail=f"無効な運行IDです: {rid}")
        pairs = []
    else:
        pairs = body.get("pairs") or []
        for pair in pairs:
            id1 = str(pair.get("運行ID1") or "").strip()
            id2 = str(pair.get("運行ID2") or "").strip()
            n1, n2 = _normalize_run_id(id1), _normalize_run_id(id2)
            if n1 not in run_id_to_index or n2 not in run_id_to_index or n1 == n2:
                raise HTTPException(status_code=400, detail=f"無効なペアです: 運行ID {id1} と {id2}")
    inp_dir = job_input_dir(jobId)
    alcohol_events = integrate_alcohol(inp_dir / "taimen", inp_dir / "alcohol")
    if alcohol_events:
        crew_in_digitaco = _crew_ids_in_run_states(run_states)
        alcohol_only = alcohol_only_crew_list(alcohol_events, crew_in_digitaco)
        if alcohol_only:
            driver_rows = [
                {"rowIndex": i, "運行ID": (rs.get("merged_header") or {}).get("運行ID"), "乗務員ID": (rs.get("merged_header") or {}).get("乗務員ID"), "乗務員名": (rs.get("merged_header") or {}).get("乗務員名"), "出庫日時": (rs.get("merged_header") or {}).get("出庫日時") or "", "帰庫日時": (rs.get("merged_header") or {}).get("帰庫日時") or ""}
                for i, rs in enumerate(run_states)
            ]
            manual_data = {"run_states": run_states, "headers": headers, "mergeGroups": merge_groups, "runDateChoices": run_date_choices, "linkPairs": pairs, "alcoholOnlyCrew": alcohol_only, "driverRows": driver_rows}
            if data.get("linkRuns") is not None:
                manual_data["linkRuns"] = data["linkRuns"]
            if link_groups_arg is not None:
                manual_data["linkGroups"] = link_groups_arg
            if merge_sets is not None:
                manual_data["mergeSets"] = merge_sets
            else:
                manual_data["mergeChoices"] = merge_choices
            manual_path.write_text(json.dumps(manual_data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            state.status = "codriver_link_required"
            state.pendingRows = None
            state.artifacts = Artifacts(excel=False, log=True, skipped=True)
            save_state(sp, state)
            return {"ok": True, "status": "codriver_link_required", "message": "アルコールのデータのみの乗務員がいます。同乗者としてどの運行に紐づけますか？"}
        apply_alcohol_to_run_states(run_states, alcohol_events, margin_minutes=120)
    new_rows = rows_from_run_states(run_states, headers, preset_path, state.device)
    return _after_link_decision(
        jobId, sp, state, run_states, headers, new_rows,
        merge_groups, merge_choices, run_date_choices, pairs, merge_sets=merge_sets, came_from="link_decision_required",
        link_groups=link_groups_arg,
        link_runs=data.get("linkRuns"),
    )


@app.post("/api/jobs/{jobId}/complete-codriver-skip")
def complete_codriver_skip(jobId: str):
    """同乗者紐づけをスキップ。アルコール突合 → B リスト or ⑤。"""
    sp = job_state_path(jobId)
    if not sp.exists():
        raise HTTPException(status_code=404, detail="Job not found.")
    state = load_state(sp)
    if state.status != "codriver_link_required":
        raise HTTPException(status_code=400, detail="このジョブは同乗者紐づけ待ちではありません。")
    out_dir = job_output_dir(jobId)
    manual_path = out_dir / "manual_input_state.json"
    if not manual_path.exists():
        raise HTTPException(status_code=404, detail="手入力状態が見つかりません。")
    data = json.loads(manual_path.read_text(encoding="utf-8"))
    run_states = data.get("run_states") or []
    headers = data.get("headers") or []
    merge_groups = data.get("mergeGroups") or []
    merge_choices = data.get("mergeChoices") or []
    merge_sets = data.get("mergeSets")
    run_date_choices = data.get("runDateChoices") or []
    link_pairs = data.get("linkPairs") or []
    if not run_states or not headers:
        raise HTTPException(status_code=400, detail="手入力状態が不正です。")
    preset_path = COMPANIES_DIR / state.company / f"{state.device}.json"
    if not preset_path.exists():
        raise HTTPException(status_code=400, detail="プリセットが見つかりません。")
    inp_dir = job_input_dir(jobId)
    alcohol_events = integrate_alcohol(inp_dir / "taimen", inp_dir / "alcohol")
    if alcohol_events:
        apply_alcohol_to_run_states(run_states, alcohol_events, margin_minutes=120)
    new_rows = rows_from_run_states(run_states, headers, preset_path, state.device)
    return _after_link_decision(
        jobId, sp, state, run_states, headers, new_rows,
        merge_groups, merge_choices, run_date_choices, link_pairs, [], merge_sets=merge_sets, came_from="codriver_link_required",
        link_runs=data.get("linkRuns"),
    )


@app.post("/api/jobs/{jobId}/complete-codriver-link")
def complete_codriver_link(jobId: str, body: Dict[str, Any] = Body(...)):
    """同乗者紐づけを確定。links: [{ 乗務員ID正規化, runIndex, driverRowIndex }, ...]。"""
    sp = job_state_path(jobId)
    if not sp.exists():
        raise HTTPException(status_code=404, detail="Job not found.")
    state = load_state(sp)
    if state.status != "codriver_link_required":
        raise HTTPException(status_code=400, detail="このジョブは同乗者紐づけ待ちではありません。")
    out_dir = job_output_dir(jobId)
    manual_path = out_dir / "manual_input_state.json"
    if not manual_path.exists():
        raise HTTPException(status_code=404, detail="手入力状態が見つかりません。")
    data = json.loads(manual_path.read_text(encoding="utf-8"))
    run_states = data.get("run_states") or []
    headers = data.get("headers") or []
    merge_groups = data.get("mergeGroups") or []
    merge_choices = data.get("mergeChoices") or []
    merge_sets = data.get("mergeSets")
    run_date_choices = data.get("runDateChoices") or []
    link_pairs = data.get("linkPairs") or []
    alcohol_only_crew = data.get("alcoholOnlyCrew") or []
    if not run_states or not headers:
        raise HTTPException(status_code=400, detail="手入力状態が不正です。")
    preset_path = COMPANIES_DIR / state.company / f"{state.device}.json"
    if not preset_path.exists():
        raise HTTPException(status_code=400, detail="プリセットが見つかりません。")
    crew_by_norm = {c["乗務員ID正規化"]: c for c in alcohol_only_crew}
    resolved: List[Dict[str, Any]] = []
    for link in body.get("links") or []:
        crew_norm = link.get("乗務員ID正規化") or link.get("crewNorm")
        run_index = int(link.get("runIndex", 0))
        driver_row_index = int(link.get("driverRowIndex", -1))
        if driver_row_index < 0 or driver_row_index >= len(run_states):
            continue
        c = crew_by_norm.get(crew_norm)
        if not c or not c.get("runs"):
            continue
        runs = c["runs"]
        if run_index < 0 or run_index >= len(runs):
            run_index = 0
        r = runs[run_index]
        resolved.append({
            "乗務員ID": c.get("乗務員ID"),
            "乗務員名": c.get("乗務員名"),
            "出庫日時": r.get("出庫日時") or "",
            "帰庫日時": r.get("帰庫日時") or "",
            "driverRowIndex": driver_row_index,
        })
    manual_data = {"run_states": run_states, "headers": headers, "mergeGroups": merge_groups, "runDateChoices": run_date_choices, "linkPairs": link_pairs, "codriverLinks": resolved}
    if data.get("linkRuns") is not None:
        manual_data["linkRuns"] = data["linkRuns"]
    if merge_sets is not None:
        manual_data["mergeSets"] = merge_sets
    else:
        manual_data["mergeChoices"] = merge_choices
    manual_path.write_text(json.dumps(manual_data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    inp_dir = job_input_dir(jobId)
    alcohol_events = integrate_alcohol(inp_dir / "taimen", inp_dir / "alcohol")
    if alcohol_events:
        apply_alcohol_to_run_states(run_states, alcohol_events, margin_minutes=120)
    new_rows = rows_from_run_states(run_states, headers, preset_path, state.device)
    return _after_link_decision(
        jobId, sp, state, run_states, headers, new_rows,
        merge_groups, merge_choices, run_date_choices, link_pairs, resolved, merge_sets=merge_sets, came_from="codriver_link_required",
        link_runs=data.get("linkRuns"),
    )


@app.post("/api/jobs/{jobId}/complete-manual")
def complete_manual(jobId: str, body: Dict[str, Any] = Body(...)):
    """手入力（B）を反映し、⑤（3h未満合算→3h以上ペア合算）を実行して Excel または再手入力へ。"""
    sp = job_state_path(jobId)
    if not sp.exists():
        raise HTTPException(status_code=404, detail="Job not found.")
    state = load_state(sp)
    if state.status != "manual_input_required":
        raise HTTPException(status_code=400, detail="このジョブは手入力待ちではありません。")
    entries = body.get("entries") or []
    if not entries:
        raise HTTPException(status_code=400, detail="entries を指定してください。")
    out_dir = job_output_dir(jobId)
    state_path = out_dir / "manual_input_state.json"
    if not state_path.exists():
        raise HTTPException(status_code=404, detail="手入力状態が見つかりません。")
    data = json.loads(state_path.read_text(encoding="utf-8"))
    run_states = data.get("run_states") or []
    headers = data.get("headers") or []
    if not run_states or not headers:
        raise HTTPException(status_code=400, detail="手入力状態が不正です。")
    preset_path = COMPANIES_DIR / state.company / f"{state.device}.json"
    if not preset_path.exists():
        raise HTTPException(status_code=400, detail="プリセットが見つかりません。")
    merge_groups = data.get("mergeGroups") or []
    merge_choices = data.get("mergeChoices") or []
    merge_sets = data.get("mergeSets")
    run_date_choices = data.get("runDateChoices") or []
    link_pairs = data.get("linkPairs") or []
    link_groups = data.get("linkGroups")
    codriver_links = data.get("codriverLinks") or []
    if merge_groups or link_pairs or link_groups:
        _apply_entries_to_run_states(run_states, entries, merge_groups, merge_choices, merge_sets)
        return _do_merge_and_excel(
            jobId, sp, state, run_states, headers,
            merge_groups, merge_choices, run_date_choices, link_pairs, codriver_links, merge_sets,
            link_groups=link_groups,
            from_complete_manual=True,
        )
    excel_path = out_dir / "output.xlsx"
    if codriver_links:
        _apply_entries_to_run_states(run_states, entries, [], [], merge_sets)
        rows = rows_from_run_states(run_states, headers, preset_path, state.device)
        rows = rows + _build_codriver_rows(rows, codriver_links)
        write_excel(headers, rows, excel_path)
    else:
        complete_manual_input(run_states, headers, entries, preset_path, state.device, excel_path)
    state.status = "succeeded"
    state.pendingRows = None
    state.artifacts = Artifacts(excel=True, log=True, skipped=True)
    save_state(sp, state)
    return {"ok": True, "status": "succeeded", "message": "手入力を反映し、Excel を出力しました。"}


def _artifact_path(jobId: str, kind: str) -> Path:
    out = job_output_dir(jobId)
    if kind == "excel":
        return out / "output.xlsx"
    if kind == "log":
        return out / "log.csv"
    raise HTTPException(status_code=404, detail="Unknown artifact.")

@app.get("/api/jobs/{jobId}/download/{kind}")
def download(jobId: str, kind: str):
    path = _artifact_path(jobId, kind)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Artifact not ready.")
    media = {
        "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "log": "text/csv; charset=utf-8",
    }[kind]
    return FileResponse(path, media_type=media, filename=path.name)


# 静的フロント（HTML/CSS/JS）をルートで配信。API は /api/* で先に定義済みのため優先される。
# exe 化時は exe 横の _app/web を参照（複数候補を試す）。
_candidates: list = []
if getattr(sys, "frozen", False):
    _candidates = [
        Path(sys.executable).resolve().parent / "_app" / "web",
        Path(sys.argv[0]).resolve().parent / "_app" / "web",
        Path.cwd().resolve() / "_app" / "web",
    ]
    _WEB_DIR = next((p for p in _candidates if p.is_dir()), _candidates[0])
else:
    _WEB_DIR = Path(__file__).resolve().parent.parent / "web"
if _WEB_DIR.is_dir():
    app.mount("/", StaticFiles(directory=str(_WEB_DIR), html=True), name="static")
else:
    # exe 時のみ: 静的が無い場合にデバッグ用エンドポイントを追加
    _tried = _candidates
    @app.get("/api/debug/web-path")
    def _debug_web_path():
        return {
            "frozen": getattr(sys, "frozen", False),
            "sys_executable": str(getattr(sys, "executable", "")),
            "cwd": str(Path.cwd().resolve()),
            "tried": [str(p) for p in _tried],
            "web_dir": str(_WEB_DIR),
            "exists": _WEB_DIR.exists(),
            "is_dir": _WEB_DIR.is_dir(),
        }
