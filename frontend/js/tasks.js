(function () {
  "use strict";

  let tasks = [];

  async function loadTasks() {
    Common.clearAlert();
    try {
      const response = await Api.get("/api/tasks", { limit: 100, offset: 0 });
      if (!response.ok) {
        Common.showError(await Api.errorMessage(response));
        return;
      }
      const body = await response.json();
      tasks = body.items || [];
      renderTasks();
    } catch (e) {
      Common.showError(e.message);
    }
  }

  function renderTasks() {
    const tbody = document.getElementById("tasks-body");
    const empty = document.getElementById("tasks-empty");
    tbody.innerHTML = "";

    if (!tasks.length) {
      empty.style.display = "block";
      return;
    }
    empty.style.display = "none";

    const me = Auth.currentUser();
    tasks.forEach(function (task) {
      const tr = document.createElement("tr");

      const tdTitle = document.createElement("td");
      tdTitle.textContent = task.title;
      tr.appendChild(tdTitle);

      const tdStatus = document.createElement("td");
      const badge = document.createElement("span");
      badge.className = task.status === "completed" ? "badge badge-completed" : "badge badge-open";
      badge.textContent = task.status === "completed" ? "выполнена" : "открыта";
      tdStatus.appendChild(badge);
      tr.appendChild(tdStatus);

      const tdReward = document.createElement("td");
      tdReward.textContent = Common.money(task.completeReward);
      tr.appendChild(tdReward);

      const tdAction = document.createElement("td");
      if (task.status === "open" && me && task.assigneeId === me.id) {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "btn-secondary btn-small";
        btn.textContent = "Выполнить";
        btn.addEventListener("click", function () {
          completeTask(task.id);
        });
        tdAction.appendChild(btn);
      }
      tr.appendChild(tdAction);

      tbody.appendChild(tr);
    });
  }

  async function createTask(title, description) {
    Common.clearAlert();
    const body = { title: title };
    if (description) {
      body.description = description;
    }
    try {
      const response = await Api.post("/api/tasks", body);
      if (!response.ok) {
        Common.showError(await Api.errorMessage(response));
        return;
      }
      Common.showSuccess("Задача создана");
      await loadTasks();
    } catch (e) {
      Common.showError(e.message);
    }
  }

  async function completeTask(taskId) {
    Common.clearAlert();
    try {
      const response = await Api.patch("/api/tasks/" + taskId + "/complete", {});
      if (!response.ok) {
        Common.showError(await Api.errorMessage(response));
        return;
      }
      Common.showSuccess("Задача выполнена");
      await loadTasks();
    } catch (e) {
      Common.showError(e.message);
    }
  }

  async function shuffle() {
    Common.clearAlert();
    try {
      const response = await Api.post("/api/tasks/shuffle", {});
      if (!response.ok) {
        Common.showError(await Api.errorMessage(response));
        return;
      }
      const body = await response.json();
      Common.showSuccess("Заассайнено задач: " + body.tasksReassigned);
      await loadTasks();
    } catch (e) {
      Common.showError(e.message);
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    if (!document.getElementById("tasks-body")) {
      return;
    }

    if (Auth.isManagerOrAdmin()) {
      document.getElementById("btn-shuffle").style.display = "inline-block";
      document.getElementById("btn-shuffle").addEventListener("click", shuffle);
    }

    document.getElementById("create-task-form").addEventListener("submit", function (event) {
      event.preventDefault();
      const title = document.getElementById("task-title").value.trim();
      const description = document.getElementById("task-description").value.trim();
      if (!title) {
        Common.showError("Введите название задачи");
        return;
      }
      createTask(title, description);
      document.getElementById("task-title").value = "";
      document.getElementById("task-description").value = "";
    });

    loadTasks();
  });
})();
