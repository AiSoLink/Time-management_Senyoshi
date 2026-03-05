from __future__ import annotations
import json
from pathlib import Path
from typing import List

from storage.paths import job_input_dir, job_output_dir, job_state_path, COMPANIES_DIR
from storage.state import Artifacts, save_state, iso_now, load_state
from engine.pipeline import run_pipeline
from engine.alcohol_integration import integrate_alcohol, write_integrated_excel

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
