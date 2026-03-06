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
      } catch (e) {}
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
})();
