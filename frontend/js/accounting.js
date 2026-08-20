(function () {
  "use strict";

  const AUDIT_LABELS = {
    assign: "Ассайн",
    complete: "Выполнение",
    refund: "Возврат",
    payout: "Выплата"
  };

  function todayIso() {
    const d = new Date();
    return d.getFullYear() + "-" + String(d.getMonth() + 1).padStart(2, "0") + "-" + String(d.getDate()).padStart(2, "0");
  }

  async function loadBalance() {
    try {
      const response = await Api.get("/api/accounts/me/balance");
      if (!response.ok) {
        Common.showError(await Api.errorMessage(response));
        return;
      }
      const body = await response.json();
      const el = document.getElementById("balance-value");
      el.textContent = Common.money(body.balance);
      el.className = "stat " + Common.moneyClass(body.balance);
    } catch (e) {
      Common.showError(e.message);
    }
  }

  async function loadAuditLog() {
    try {
      const response = await Api.get("/api/accounts/me/audit-log", { limit: 100, offset: 0 });
      if (!response.ok) {
        Common.showError(await Api.errorMessage(response));
        return;
      }
      const body = await response.json();
      const items = body.items || [];
      const tbody = document.getElementById("audit-body");
      const empty = document.getElementById("audit-empty");
      tbody.innerHTML = "";

      if (!items.length) {
        empty.style.display = "block";
        return;
      }
      empty.style.display = "none";

      items.forEach(function (entry) {
        const tr = document.createElement("tr");

        const tdTime = document.createElement("td");
        tdTime.textContent = Common.dateTime(entry.timestamp);
        tr.appendChild(tdTime);

        const tdType = document.createElement("td");
        tdType.textContent = AUDIT_LABELS[entry.type] || entry.type;
        tr.appendChild(tdType);

        const tdAmount = document.createElement("td");
        tdAmount.textContent = Common.money(entry.amount);
        tdAmount.className = Common.moneyClass(entry.amount);
        tr.appendChild(tdAmount);

        const tdDescription = document.createElement("td");
        tdDescription.textContent = entry.description || "";
        tr.appendChild(tdDescription);

        tbody.appendChild(tr);
      });
    } catch (e) {
      Common.showError(e.message);
    }
  }

  async function loadManagementEarnings(date) {
    try {
      const response = await Api.get("/api/accounts/top-management-earnings", { date: date });
      if (!response.ok) {
        Common.showError(await Api.errorMessage(response));
        return;
      }
      const body = await response.json();
      const el = document.getElementById("mgmt-earnings-value");
      el.textContent = Common.money(body.amount);
      el.className = "stat " + Common.moneyClass(body.amount);
    } catch (e) {
      Common.showError(e.message);
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    if (!document.getElementById("balance-value")) {
      return;
    }

    loadBalance();
    loadAuditLog();

    if (Auth.isAccountantOrAdmin()) {
      const card = document.getElementById("mgmt-earnings-card");
      card.style.display = "block";
      const dateInput = document.getElementById("mgmt-earnings-date");
      dateInput.value = todayIso();
      document.getElementById("mgmt-earnings-form").addEventListener("submit", function (event) {
        event.preventDefault();
        loadManagementEarnings(dateInput.value);
      });
    }
  });
})();
