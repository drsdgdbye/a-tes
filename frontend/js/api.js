(function () {
  "use strict";

  const TOKEN_KEY = "ates_access_token";
  const REFRESH_KEY = "ates_refresh_token";

  const Api = {
    TOKEN_KEY: TOKEN_KEY,
    REFRESH_KEY: REFRESH_KEY,

    getToken: function () {
      return localStorage.getItem(TOKEN_KEY);
    },
    getRefreshToken: function () {
      return localStorage.getItem(REFRESH_KEY);
    },
    setTokens: function (tokens) {
      localStorage.setItem(TOKEN_KEY, tokens.accessToken);
      localStorage.setItem(REFRESH_KEY, tokens.refreshToken);
    },
    clearTokens: function () {
      localStorage.removeItem(TOKEN_KEY);
      localStorage.removeItem(REFRESH_KEY);
    },

    // Выполнить запрос к API. При 401 — обновить access-токен и повторить один раз.
    request: async function (path, options) {
      options = options || {};
      const headers = Object.assign({ "Content-Type": "application/json" }, options.headers || {});
      const token = this.getToken();
      if (token) {
        headers["Authorization"] = "Bearer " + token;
      }

      let response = await fetch(path, {
        method: options.method || "GET",
        headers: headers,
        body: options.body !== undefined ? JSON.stringify(options.body) : undefined
      });

      if (response.status === 401 && this.getRefreshToken()) {
        const refreshed = await this.tryRefresh();
        if (refreshed) {
          headers["Authorization"] = "Bearer " + this.getToken();
          response = await fetch(path, {
            method: options.method || "GET",
            headers: headers,
            body: options.body !== undefined ? JSON.stringify(options.body) : undefined
          });
        }
      }

      if (response.status === 401) {
        this.redirectToLogin();
        throw new Error("UNAUTHORIZED");
      }

      return response;
    },

    // Обновить пару токенов; дедуплицирует параллельные вызовы refresh.
    tryRefresh: async function () {
      const refreshToken = this.getRefreshToken();
      if (!refreshToken) {
        return false;
      }
      if (!this._refreshing) {
        this._refreshing = this.doRefresh(refreshToken)
          .then(
            function (tokens) {
              this.setTokens(tokens);
              return true;
            }.bind(this),
            function () {
              this.clearTokens();
              return false;
            }.bind(this)
          )
          .finally(
            function () {
              this._refreshing = null;
            }.bind(this)
          );
      }
      return this._refreshing;
    },

    doRefresh: async function (refreshToken) {
      const response = await fetch("/api/auth/refresh", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ refreshToken: refreshToken })
      });
      if (!response.ok) {
        throw new Error("REFRESH_FAILED");
      }
      return response.json();
    },

    logout: async function () {
      const refreshToken = this.getRefreshToken();
      try {
        if (refreshToken) {
          await fetch("/api/auth/logout", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ refreshToken: refreshToken })
          });
        }
      } finally {
        this.clearTokens();
      }
    },

    redirectToLogin: function () {
      this.clearTokens();
      if (window.location.pathname !== "/index.html" && window.location.pathname !== "/") {
        window.location.href = "/";
      }
    },

    // Парсит тело ошибки в читаемое сообщение, иначе — HTTP-статус.
    errorMessage: async function (response) {
      try {
        const body = await response.json();
        if (body && body.message) {
          return body.message;
        }
        if (body && body.error) {
          return body.error;
        }
      } catch (_e) {
        // тело не JSON — используем статус
      }
      return "HTTP " + response.status;
    },

    get: function (path, query) {
      const qs = this.queryString(query);
      return this.request(path + qs);
    },
    post: function (path, body) {
      return this.request(path, { method: "POST", body: body });
    },
    patch: function (path, body) {
      return this.request(path, { method: "PATCH", body: body });
    },
    queryString: function (query) {
      if (!query) {
        return "";
      }
      const params = Object.keys(query)
        .filter(function (key) {
          return query[key] !== undefined && query[key] !== null && query[key] !== "";
        })
        .map(function (key) {
          return encodeURIComponent(key) + "=" + encodeURIComponent(query[key]);
        });
      return params.length ? "?" + params.join("&") : "";
    }
  };

  window.Api = Api;
})();
