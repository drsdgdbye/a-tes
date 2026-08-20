(function () {
  "use strict";

  function todayIso() {
    const d = new Date();
    return d.getFullYear() + "-" + String(d.getMonth() + 1).padStart(2, "0") + "-" + String(d.getDate()).padStart(2, "0");
  }

  function addDays(iso, days) {
    const d = new Date(iso + "T00:00:00Z");
    d.setUTCDate(d.getUTCDate() + days);
    return d.toISOString().split("T")[0];
  }

  async function loadManagementEarnings(from, to) {
    try {
      const response = await Api.get("/api/analytics/top-management-earnings", { from: from, to: to });
      if (!response.ok) {
        Common.showError(await Api.errorMessage(response));
        return;
      }
      const body = await response.json();
      const tbody = document.getElementById("mgmt-body");
      tbody.innerHTML = "";
      (body.items || []).forEach(function (item) {
        const tr = document.createElement("tr");
        const tdDate = document.createElement("td");
        tdDate.textContent = Common.date(item.date);
        const tdAmount = document.createElement("td");
        tdAmount.textContent = Common.money(item.amount);
        tdAmount.className = Common.moneyClass(item.amount);
        tr.appendChild(tdDate);
        tr.appendChild(tdAmount);
        tbody.appendChild(tr);
      });
      document.getElementById("mgmt-total").textContent = "Итого: " + Common.money(body.total);
    } catch (e) {
      Common.showError(e.message);
    }
  }

  async function loadPopugsInMinus() {
    try {
      const response = await Api.get("/api/analytics/popugs-in-minus");
      if (!response.ok) {
        Common.showError(await Api.errorMessage(response));
        return;
      }
      const body = await response.json();
      const tbody = document.getElementById("minus-body");
      const empty = document.getElementById("minus-empty");
      tbody.innerHTML = "";
      const items = body.items || [];

      if (!items.length) {
        empty.style.display = "block";
        return;
      }
      empty.style.display = "none";

      items.forEach(function (item) {
        const tr = document.createElement("tr");
        const tdName = document.createElement("td");
        tdName.textContent = item.name;
        const tdBalance = document.createElement("td");
        tdBalance.textContent = Common.money(item.balance);
        tdBalance.className = Common.moneyClass(item.balance);
        tr.appendChild(tdName);
        tr.appendChild(tdBalance);
        tbody.appendChild(tr);
      });
    } catch (e) {
      Common.showError(e.message);
    }
  }

  async function loadMostExpensive(period, date) {
    try {
      const response = await Api.get("/api/analytics/most-expensive-task", { period: period, date: date });
      if (!response.ok) {
        Common.showError(await Api.errorMessage(response));
        return;
      }
      const body = await response.json();
      const tbody = document.getElementById("expensive-body");
      tbody.innerHTML = "";
      (body.items || []).forEach(function (item) {
        const tr = document.createElement("tr");
        const tdDate = document.createElement("td");
        tdDate.textContent = Common.date(item.date);
        const tdTitle = document.createElement("td");
        tdTitle.textContent = item.title;
        const tdAmount = document.createElement("td");
        tdAmount.textContent = Common.money(item.amount);
        tr.appendChild(tdDate);
        tr.appendChild(tdTitle);
        tr.appendChild(tdAmount);
        tbody.appendChild(tr);
      });
      if (body.overall) {
        document.getElementById("expensive-overall").textContent =
          "Максимум за период: " + Common.money(body.overall.amount) + " («" + body.overall.title + "»)";
      } else {
        document.getElementById("expensive-overall").textContent = "За период закрытых задач нет";
      }
    } catch (e) {
      Common.showError(e.message);
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    if (!document.getElementById("mgmt-body")) {
      return;
    }

    if (!Auth.isAdmin()) {
      Common.showError("Аналитика доступна только администраторам");
      document.querySelectorAll(".container .card").forEach(function (card) {
        card.style.display = "none";
      });
      return;
    }

    const fromInput = document.getElementById("mgmt-from");
    const toInput = document.getElementById("mgmt-to");
    const today = todayIso();
    fromInput.value = addDays(today, -6);
    toInput.value = today;
    loadManagementEarnings(fromInput.value, toInput.value);

    document.getElementById("mgmt-form").addEventListener("submit", function (event) {
      event.preventDefault();
      loadManagementEarnings(fromInput.value, toInput.value);
    });

    loadPopugsInMinus();

    const periodSelect = document.getElementById("expensive-period");
    const dateInput = document.getElementById("expensive-date");
    dateInput.value = today;
    loadMostExpensive(periodSelect.value, dateInput.value);

    document.getElementById("expensive-form").addEventListener("submit", function (event) {
      event.preventDefault();
      loadMostExpensive(periodSelect.value, dateInput.value);
    });
  });
})();
