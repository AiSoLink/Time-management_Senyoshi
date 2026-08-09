/**
 * 静的フロント用: API 基底URL と fetch ラッパー
 */
(function () {
  const getApiBase = function () {
    if (typeof window !== "undefined" && window.API_BASE) return window.API_BASE;
    if (typeof window !== "undefined" && window.location && window.location.origin) return window.location.origin;
    return "http://localhost:8000";
  };
  window.API_BASE = getApiBase();

  function handleResponse(res) {
    return res.text().then(function (text) {
      var msg = text;
      try {
        var j = JSON.parse(text);
        if (typeof j.detail === "string") msg = j.detail;
      } catch (e) {
        console.error("JSON解析エラー:", e);
      }
      throw new Error(msg || "エラー (" + res.status + ")");
    });
  }

  window.apiGet = function (path) {
    return fetch(window.API_BASE + path, { cache: "no-store" })
      .then(function (res) {
        if (!res.ok) return handleResponse(res);
        return res.json();
      })
      .catch(function (e) {
        if (e.message && e.message.indexOf("Failed to fetch") !== -1) {
          throw new Error("バックエンドに接続できません。API サーバー（例: localhost:8000）が起動しているか確認してください。");
        }
        throw e;
      });
  };

  window.apiPostJson = function (path, body) {
    return fetch(window.API_BASE + path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
      .then(function (res) {
        if (!res.ok) return handleResponse(res);
        return res.json();
      })
      .catch(function (e) {
        if (e.message && e.message.indexOf("Failed to fetch") !== -1) {
          throw new Error("バックエンドに接続できません。API サーバー（例: localhost:8000）が起動しているか確認してください。");
        }
        throw e;
      });
  };

  /* ============ 右サイドパネル共通（フロー図＋開閉式の操作ガイド） ============ */

  var FLOW_STEPS = [
    { key: "prep",  label: "前準備",   title: "ファイルアップロード", desc: "" },
    { key: "step1", label: "ステップ1", title: "運行をまとめる",       desc: "運行が分かれちゃったものを1つにまとめる" },
    { key: "step2", label: "ステップ2", title: "横乗りを選ぶ",         desc: "だれの横乗りをしたかを選ぶ" },
    { key: "step3", label: "ステップ3", title: "時刻を手入力",         desc: "出庫や帰庫の時刻を手入力する" },
    { key: "done",  label: "完了",     title: "Excelダウンロード",     desc: "" }
  ];

  /* currentKey: "prep" | "step1" | "step2" | "step3" | "done" */
  window.renderFlow = function (currentKey) {
    var el = document.getElementById("flowContent");
    if (!el) return;
    var currentIdx = -1;
    for (var i = 0; i < FLOW_STEPS.length; i++) {
      if (FLOW_STEPS[i].key === currentKey) { currentIdx = i; break; }
    }
    var html = "";
    for (var i = 0; i < FLOW_STEPS.length; i++) {
      var s = FLOW_STEPS[i];
      var state = currentKey === "done" ? (i < FLOW_STEPS.length - 1 ? "done" : "current")
        : (i < currentIdx ? "done" : i === currentIdx ? "current" : "todo");
      var icon = state === "done" ? "✓" : state === "current" ? "▶" : "○";
      html += '<div class="flow-step flow-' + state + '">'
        + '<div class="flow-icon">' + icon + '</div>'
        + '<div class="flow-body"><div class="flow-label">' + s.label
        + (state === "current" ? '<span class="flow-now">いまここ</span>' : '')
        + '</div>'
        + '<div class="flow-title">' + s.title + (state === "done" ? '<span class="flow-done-tag">済</span>' : '') + '</div>'
        + (s.desc ? '<div class="flow-desc">' + s.desc + '</div>' : '')
        + '</div></div>';
      if (i < FLOW_STEPS.length - 1) html += '<div class="flow-connector"></div>';
    }
    el.innerHTML = html;
  };

  /* ガイドの開閉ボタンを配線する。ページ読み込み時に1回呼ぶ */
  window.initGuidePanel = function () {
    var t = document.getElementById("guideToggle");
    var g = document.getElementById("guideContent");
    if (!t || !g) return;
    t.addEventListener("click", function () {
      var open = g.style.display !== "none";
      g.style.display = open ? "none" : "block";
      t.textContent = open ? "操作ガイドを表示 ▾" : "操作ガイドを閉じる ▴";
    });
  };
})();
