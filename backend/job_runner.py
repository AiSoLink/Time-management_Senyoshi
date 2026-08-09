from __future__ import annotations
import json
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List

from storage.paths import job_input_dir, job_output_dir, job_state_path, COMPANIES_DIR
from storage.state import Artifacts, save_state, iso_now, load_state
from engine.pipeline import run_pipeline, vehicle_plate_display
from engine.alcohol_integration import integrate_alcohol, write_integrated_excel, _normalize_crew_id, _to_datetime

# 出庫〜帰庫がこの時間以上の運行は「帰庫の押し忘れ」の疑いとして確認画面に回す
LONG_RUN_HOURS = 24


def _detect_long_runs(run_states: List[Dict[str, Any]], input_dir: Path) -> List[Dict[str, Any]]:
    """拘束が LONG_RUN_HOURS 以上の運行を検知し、アルコール検査から修正・分割の候補を付けて返す。"""
    try:
        alcohol_events = integrate_alcohol(input_dir / "taimen", input_dir / "alcohol")
    except Exception:
        alcohol_events = []
    out: List[Dict[str, Any]] = []
    for i, rs in enumerate(run_states or []):
        mh = rs.get("merged_header") or {}
        t_out = _to_datetime(mh.get("出庫日時"))
        t_in = _to_datetime(mh.get("帰庫日時"))
        if not t_out or not t_in:
            continue
        hours = (t_in - t_out).total_seconds() / 3600.0
        if hours < LONG_RUN_HOURS:
            continue
        crew = _normalize_crew_id(mh.get("乗務員ID"))
        # 表示用: 運行期間内（両端含む）のアルコール検査すべて
        in_period = []
        # 候補用: 運行期間の内側（両端1時間を除く）にあるアルコール検査
        inner = []
        for e in alcohol_events:
            if _normalize_crew_id(e[0]) != crew:
                continue
            t = _to_datetime(e[2])
            if t is None:
                continue
            if (t_out - timedelta(hours=2)) <= t <= (t_in + timedelta(hours=2)):
                in_period.append((e[3], t))
            if (t_out + timedelta(hours=1)) <= t <= (t_in - timedelta(hours=1)):
                inner.append((e[3], t))
        in_period.sort(key=lambda x: x[1])
        inner.sort(key=lambda x: x[1])
        fix_candidates = [t.strftime("%Y/%m/%d %H:%M") for k, t in inner if k == "帰庫"]
        split_candidates = []
        for a in range(len(inner)):
            if inner[a][0] != "帰庫":
                continue
            for b in range(a + 1, len(inner)):
                if inner[b][0] == "出庫":
                    split_candidates.append({
                        "帰庫": inner[a][1].strftime("%Y/%m/%d %H:%M"),
                        "出庫": inner[b][1].strftime("%Y/%m/%d %H:%M"),
                    })
                    break
        out.append({
            "rowIndex": i,
            "乗務員ID": mh.get("乗務員ID"),
            "乗務員名": mh.get("乗務員名"),
            "車番": vehicle_plate_display(mh.get("車両番号")),
            "出庫日時": mh.get("出庫日時"),
            "帰庫日時": mh.get("帰庫日時"),
            "拘束時間h": round(hours, 1),
            "fixCandidates": fix_candidates,
            "splitCandidates": split_candidates,
            "alcoholEvents": [
                {"種別": k, "日時": t.strftime("%Y/%m/%d %H:%M")} for k, t in in_period
            ],
            "details": [
                {"作業": d.get("task") or "", "到着": d.get("arrival") or "", "出発": d.get("depart") or ""}
                for d in (rs.get("merged_details") or [])
            ],
        })
    return out

def run_job(job_id: str) -> None:
    state_path = job_state_path(job_id)
    state = load_state(state_path)

    state.status = "running"
    state.startedAt = iso_now()
    save_state(state_path, state)

    try:
        input_dir = job_input_dir(job_id)
        out_dir = job_output_dir(job_id)
        pdfs: List[Path] = sorted(input_dir.glob("*.pdf"))

        preset = COMPANIES_DIR / state.company / f"{state.device}.json"

        result = run_pipeline(
            company=state.company,
            device=state.device,
            preset_path=preset,
            pdf_paths=pdfs,
            job_output_dir=out_dir,
            job_input_dir=input_dir,
        )

        state.totalPdfs = len(pdfs)
        state.processedPdfs = len(pdfs)
        state.errorCount = result.error_count
        state.warnCount = result.warn_count

        if getattr(result, "merge_decision_required", False) and result.run_states is not None and result.merge_groups is not None:
            # 帰庫の押し忘れが疑われる長時間運行があれば、先に確認画面（ステップ0）へ回す
            long_runs = _detect_long_runs(result.run_states, input_dir)
            if long_runs:
                state.status = "long_run_check_required"
                state.artifacts = Artifacts(excel=False, log=True, skipped=True)
                manual_data = {
                    "run_states": result.run_states,
                    "headers": result.headers or [],
                    "longRuns": long_runs,
                }
                (out_dir / "manual_input_state.json").write_text(
                    json.dumps(manual_data, ensure_ascii=False, indent=2, default=str),
                    encoding="utf-8",
                )
                state.finishedAt = iso_now()
                save_state(state_path, state)
                return
            state.status = "merge_decision_required"
            state.artifacts = Artifacts(excel=False, log=True, skipped=True)
            manual_data = {
                "run_states": result.run_states,
                "headers": result.headers or [],
                "mergeGroups": result.merge_groups,
            }
            (out_dir / "manual_input_state.json").write_text(
                json.dumps(manual_data, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
            state.finishedAt = iso_now()
            save_state(state_path, state)
            return

        if getattr(result, "manual_input_required", False) and result.run_states is not None and result.pending_rows is not None:
            state.status = "manual_input_required"
            state.pendingRows = result.pending_rows
            state.artifacts = Artifacts(excel=False, log=True, skipped=True)
            pending_indices = {p["rowIndex"] for p in result.pending_rows}
            driver_rows = [
                {
                    "rowIndex": i,
                    "運行ID": (rs.get("merged_header") or {}).get("運行ID"),
                    "乗務員ID": (rs.get("merged_header") or {}).get("乗務員ID"),
                    "乗務員名": (rs.get("merged_header") or {}).get("乗務員名"),
                    "出庫日時": (rs.get("merged_header") or {}).get("出庫日時") or "",
                    "帰庫日時": (rs.get("merged_header") or {}).get("帰庫日時") or "",
                }
                for i, rs in enumerate(result.run_states)
                if i not in pending_indices
            ]
            manual_data = {
                "run_states": result.run_states,
                "headers": result.headers or [],
                "driverRows": driver_rows,
                "alcoholRunsByCrew": getattr(result, "alcohol_runs_by_crew", None) or {},
            }
            (out_dir / "manual_input_state.json").write_text(
                json.dumps(manual_data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            state.finishedAt = iso_now()
            save_state(state_path, state)
            return

        state.artifacts = Artifacts(excel=True, log=True, skipped=True)
        # アルコール統合イベント（乗務員ID順・日時昇順、種別付き）を確認用に出力
        taimen_dir = input_dir / "taimen"
        alcohol_dir = input_dir / "alcohol"
        alcohol_events = integrate_alcohol(taimen_dir, alcohol_dir)
        write_integrated_excel(alcohol_events, out_dir / "alcohol_integrated.xlsx")

        state.status = "succeeded"
        state.finishedAt = iso_now()
        save_state(state_path, state)

    except Exception:
        state.status = "failed"
        state.errorCount += 1
        state.finishedAt = iso_now()
        save_state(state_path, state)
        raise
