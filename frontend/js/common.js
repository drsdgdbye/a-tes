(function () {
  "use strict";

  function showAlert(text, kind) {
    const el = document.getElementById("message");
    if (!el) {
      return;
    }
    el.className = "alert " + kind;
    el.textContent = text;
    window.scrollTo(0, 0);
  }

  const Common = {
    showError: function (text) {
      showAlert(text, "error");
    },
    showSuccess: function (text) {
      showAlert(text, "success");
    },
    clearAlert: function () {
      const el = document.getElementById("message");
      if (el) {
        el.className = "alert";
        el.textContent = "";
      }
    },
    // Деньги приходят строкой "25.00"/"-25.00" — просто добавляем знак $.
    money: function (value) {
      if (value === null || value === undefined) {
        return "—";
      }
      const s = String(value);
      return s.indexOf("$") === -1 ? "$" + s : s;
    },
    moneyClass: function (value) {
      const s = String(value || "0");
      if (s.indexOf("-") === 0) {
        return "negative";
      }
      if (parseFloat(s) > 0) {
        return "positive";
      }
      return "";
    },
    // ISO-8601 UTC -> локальное время (день.месяц.год ЧЧ:ММ).
    dateTime: function (iso) {
      if (!iso) {
        return "—";
      }
      const d = new Date(iso);
      if (isNaN(d.getTime())) {
        return iso;
      }
      return d.toLocaleString("ru-RU", {
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit"
      });
    },
    // "2026-01-15" -> "15.01.2026".
    date: function (iso) {
      if (!iso) {
        return "—";
      }
      const parts = String(iso).split("T")[0].split("-");
      if (parts.length !== 3) {
        return iso;
      }
      return parts[2] + "." + parts[1] + "." + parts[0];
    },
    roleLabel: {
      popug: "Попуг",
      manager: "Менеджер",
      accountant: "Бухгалтер",
      admin: "Админ"
    },
    // Навигация с учётом ролей; markActive — путь текущей страницы.
    renderNav: function () {
      const nav = document.getElementById("navbar");
      if (!nav) {
        return;
      }
      if (!Auth.currentUser()) {
        Api.redirectToLogin();
        return;
      }
      const user = Auth.currentUser();
      const path = window.location.pathname;
      const links = [
        { href: "/tasks.html", label: "Таск-трекер", show: true },
        { href: "/accounting.html", label: "Аккаунтинг", show: true },
        { href: "/analytics.html", label: "Аналитика", show: Auth.isAdmin() }
      ];
      let html = '<a class="brand" href="/tasks.html">aTES</a>';
      links.forEach(function (link) {
        if (!link.show) {
          return;
        }
        const active = path.indexOf(link.href) !== -1 ? " active" : "";
        html += '<a class="' + active.trim() + '" href="' + link.href + '">' + link.label + "</a>";
      });
      html += '<span class="spacer"></span>';
      html += '<span class="user">' + (Common.roleLabel[user.role] || user.role) + "</span>";
      html += '<button type="button" id="btn-logout" class="btn-secondary btn-small">Выйти</button>';
      nav.innerHTML = html;
      document.getElementById("btn-logout").addEventListener("click", async function () {
        await Api.logout();
        window.location.href = "/";
      });
    }
  };

  window.Common = Common;

  document.addEventListener("DOMContentLoaded", function () {
    if (document.getElementById("navbar")) {
      Common.renderNav();
    }
  });
})();
