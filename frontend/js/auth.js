(function () {
  "use strict";

  // Декод payload JWT (access-токен): { sub: userId, role: "popug"|... }.
  function decodeJwt(token) {
    try {
      const payload = token.split(".")[1];
      const normalized = payload.replace(/-/g, "+").replace(/_/g, "/");
      const padded = normalized + "=".repeat((4 - (normalized.length % 4)) % 4);
      const json = decodeURIComponent(
        atob(padded)
          .split("")
          .map(function (c) {
            return "%" + ("00" + c.charCodeAt(0).toString(16)).slice(-2);
          })
          .join("")
      );
      return JSON.parse(json);
    } catch (_e) {
      return null;
    }
  }

  const Auth = {
    currentUser: function () {
      const token = Api.getToken();
      if (!token) {
        return null;
      }
      const payload = decodeJwt(token);
      if (!payload) {
        return null;
      }
      return { id: payload.sub, role: payload.role };
    },
    hasRole: function (roles) {
      const user = this.currentUser();
      return !!user && roles.indexOf(user.role) !== -1;
    },
    isAdmin: function () {
      return this.hasRole(["admin"]);
    },
    isManagerOrAdmin: function () {
      return this.hasRole(["manager", "admin"]);
    },
    isAccountantOrAdmin: function () {
      return this.hasRole(["accountant", "admin"]);
    }
  };

  window.Auth = Auth;

  // === Логин-страница ===
  async function initLoginPage() {
    if (Auth.currentUser()) {
      window.location.href = "/tasks.html";
      return;
    }

    const message = document.getElementById("message");

    function showError(text) {
      message.className = "alert error";
      message.textContent = text;
    }

    function showSuccess(text) {
      message.className = "alert success";
      message.textContent = text;
    }

    try {
      const response = await Api.get("/api/auth/config");
      if (response.ok) {
        const config = await response.json();
        if (config.registrationEnabled) {
          document.getElementById("register-form").style.display = "block";
        }
      }
    } catch (_e) {
      // конфиг недоступен — прячем регистрацию
    }

    async function submitLogin() {
      const email = document.getElementById("login-email").value.trim();
      const password = document.getElementById("login-password").value;
      if (!email || !password) {
        showError("Введите email и пароль");
        return;
      }
      try {
        const response = await Api.post("/api/auth/login", { login: email, password: password });
        if (!response.ok) {
          showError(await Api.errorMessage(response));
          return;
        }
        const tokens = await response.json();
        Api.setTokens(tokens);
        window.location.href = "/tasks.html";
      } catch (e) {
        showError(e.message);
      }
    }

    async function submitRegister() {
      const name = document.getElementById("reg-name").value.trim();
      const email = document.getElementById("reg-email").value.trim();
      const password = document.getElementById("reg-password").value;
      if (!name || !email || !password) {
        showError("Заполните все поля");
        return;
      }
      try {
        const response = await Api.post("/api/auth/register", {
          name: name,
          email: email,
          password: password
        });
        if (!response.ok) {
          showError(await Api.errorMessage(response));
          return;
        }
        const tokens = await response.json();
        Api.setTokens(tokens);
        showSuccess("Регистрация успешна. Перенаправляем...");
        setTimeout(function () {
          window.location.href = "/tasks.html";
        }, 500);
      } catch (e) {
        showError(e.message);
      }
    }

    document.getElementById("btn-login").addEventListener("click", submitLogin);
    document.getElementById("btn-register").addEventListener("click", submitRegister);

    document
      .getElementById("login-password")
      .addEventListener("keydown", function (event) {
        if (event.key === "Enter") {
          submitLogin();
        }
      });
  }

  if (document.getElementById("btn-login")) {
    initLoginPage();
  }
})();
