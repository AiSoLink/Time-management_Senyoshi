"""
指定PDFをパイプラインと同じ方法で読み、ブロックごとの出庫・帰庫の出現回数、
および run_states に渡る直前の各 part の header と groups の様子を表示する。
使い方: python check_pdf_blocks.py "PDFのパス"
"""
import json
import re
import sys
import unicodedata
from pathlib import Path

import pdfplumber


def _nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s)


def _clean_for_regex(s: str) -> str:
    s = _nfkc(s)
    s = re.sub(r"[\x00-\x1F]+", " ", s)
    return s


def main():
    pdf_path = Path(r"c:\Users\sawak\AppData\Roaming\Cursor\User\workspaceStorage\42c5c379a8db8eabb0dc4a84da741481\pdfs\338e2e4f-c3cd-43cf-b459-e0b0778c8c1a\日報_0910_篠田.pdf")
    if len(sys.argv) > 1:
        pdf_path = Path(sys.argv[1])
    if not pdf_path.exists():
        print(f"ファイルが見つかりません: {pdf_path}")
        return

    # パイプラインと同じく全ページを結合
    texts = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for p in pdf.pages:
            texts.append(p.extract_text() or "")
    raw = "\n".join(texts)
    cleaned_full = _clean_for_regex(raw)

    # 運行IDで分割（mimamori の report_id_regex）
    report_id_regex = r"(ID-\d+)"
    outdt_re = r"出庫時刻\s*[:：]\s*(\d{4}/\d{1,2}/\d{1,2}\s*\d{2}:\d{2})"
    indt_re = r"帰庫時刻\s*[:：]\s*(\d{4}/\d{1,2}/\d{1,2}\s*\d{2}:\d{2})"

    he = re.compile(report_id_regex)
    lines = raw.splitlines()
    indices = []
    for i, line in enumerate(lines):
        if he.search(_nfkc(line)):
            indices.append(i)

    if not indices:
        run_blocks = [raw]
    else:
        run_blocks = []
        for k in range(len(indices)):
            start = indices[k]
            end = indices[k + 1] if k + 1 < len(indices) else len(lines)
            block_lines = lines[start:end]
            run_blocks.append("\n".join(block_lines))

    # パイプラインと同一: 各 run_block を clean して header 抽出（mimamori の正規表現）
    groups = {}
    for bi, run_block in enumerate(run_blocks):
        cleaned_block = _clean_for_regex(run_block)
        # 出庫・帰庫のみ簡易抽出（_extract_header_fields 相当）
        m_out = re.search(outdt_re, cleaned_block, re.MULTILINE)
        m_in = re.search(indt_re, cleaned_block, re.MULTILINE)
        out_dt = m_out.group(1).strip() if m_out else None
        in_dt = m_in.group(1).strip() if m_in else None
        rid_m = re.search(report_id_regex, cleaned_block)
        report_id = rid_m.group(1).strip() if rid_m else f"unknown_{bi}"
        header = {"運行ID": report_id, "出庫日時": out_dt, "帰庫日時": in_dt}
        part = {"pdf": pdf_path.name, "header": header}
        groups.setdefault(report_id, []).append(part)

    print("=== 各ブロックで抽出した header（旧ロジック: 先頭の出庫・先頭の帰庫）===")
    for bi, run_block in enumerate(run_blocks):
        cleaned_block = _clean_for_regex(run_block)
        m_out = re.search(outdt_re, cleaned_block, re.MULTILINE)
        m_in = re.search(indt_re, cleaned_block, re.MULTILINE)
        out_dt = m_out.group(1).strip() if m_out else None
        in_dt = m_in.group(1).strip() if m_in else None
        print(f"ブロック{bi+1}: 出庫日時={out_dt}, 帰庫日時={in_dt}")

    print()
    print("=== groups（report_id ごとの parts）===")
    for rid, parts in groups.items():
        print(f"  report_id={rid}, parts数={len(parts)}")
        for pi, part in enumerate(parts):
            h = part["header"]
            print(f"    part{pi+1}: 出庫日時={h.get('出庫日時')}, 帰庫日時={h.get('帰庫日時')}")

    # 同じ report_id が複数あるとマージされる。マージ後の出庫・帰庫を再現
    print()
    print("=== マージ後（同一 report_id の parts を _merge_header_preferring_left した想定）===")
    for rid, parts in groups.items():
        merged = {}
        for part in parts:
            h = part["header"]
            for k, v in h.items():
                if k not in merged or merged[k] in (None, "", 0):
                    merged[k] = v
                elif v not in (None, "") and merged[k] != v:
                    merged[k] = None
        print(f"  report_id={rid}: 出庫日時={merged.get('出庫日時')}, 帰庫日時={merged.get('帰庫日時')}")

    # 各ブロック内での「出現順」を表示（08:02 と 04:42 が組になる経路の特定）
    print()
    print("=== 各ブロック内での 出庫/帰庫 の出現順（先頭の組が採用される）===")
    out_re = re.compile(outdt_re)
    in_re = re.compile(indt_re)
    for bi, run_block in enumerate(run_blocks):
        cleaned_block = _clean_for_regex(run_block)
        events = []
        for m in out_re.finditer(cleaned_block):
            events.append((m.start(), "out", m.group(1)))
        for m in in_re.finditer(cleaned_block):
            events.append((m.start(), "in", m.group(1)))
        events.sort(key=lambda x: x[0])
        print(f"ブロック{bi+1} 出現順:", " -> ".join(f"{t}({v})" for _, t, v in events))
        if events:
            first_out = next((v for _, t, v in events if t == "out"), None)
            first_in = next((v for _, t, v in events if t == "in"), None)
            print(f"  => 旧ロジック「先頭の出庫・先頭の帰庫」の組: ({first_out}, {first_in})")
    print()
    print("=== 08:02 と 04:42 が組になる条件 ===")
    print("  どちらかのブロックで「先頭の出庫」=08:02 かつ「先頭の帰庫」=04:42 だと、そのブロックの拘束時間が約20h40mになる。")
    print("  そのようなブロックが2つあるか、同じ組が2回使われると合算で約41h20mになる。")


if __name__ == "__main__":
    main()
