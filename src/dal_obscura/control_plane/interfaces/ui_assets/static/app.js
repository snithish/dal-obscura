const tokenKey = "dal-obscura-admin-token";
const tokenForm = document.querySelector("#token-form");
const tokenInput = document.querySelector("#admin-token");
const toastRegion = document.querySelector("#toast-region");
const context = {};

if (sessionStorage.getItem(tokenKey)) {
  tokenInput.value = sessionStorage.getItem(tokenKey);
}

document.body.addEventListener("htmx:configRequest", (event) => {
  const token = sessionStorage.getItem(tokenKey);
  if (token) {
    event.detail.headers.Authorization = `Bearer ${token}`;
  }
});

tokenForm.addEventListener("submit", (event) => {
  event.preventDefault();
  sessionStorage.setItem(tokenKey, tokenInput.value.trim());
  showToast("Token set for this browser session");
  document.body.dispatchEvent(new Event("token-ready"));
});

document.addEventListener("input", (event) => {
  const contextInput = event.target.closest("[data-context-key]");
  if (contextInput) {
    context[contextInput.dataset.contextKey] = contextInput.value.trim();
  }
  const filter = event.target.closest("[data-table-filter]");
  if (filter) {
    filterTable(filter);
  }
});

document.addEventListener("click", (event) => {
  const panelButton = event.target.closest("[data-panel]");
  if (panelButton) {
    showPanel(panelButton.dataset.panel);
    return;
  }
  const refresh = event.target.closest("[data-refresh-context]");
  if (refresh) {
    refreshContextRoutes();
    return;
  }
  const sortButton = event.target.closest("[data-sort-key]");
  if (sortButton) {
    sortTable(sortButton);
  }
});

function showPanel(panelId) {
  document.querySelectorAll(".panel").forEach((panel) => {
    panel.classList.toggle("is-active", panel.id === panelId);
  });
  document.querySelectorAll("[data-panel]").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.panel === panelId);
  });
  refreshContextRoutes();
}

function refreshContextRoutes() {
  document.querySelectorAll("[data-route-template]").forEach((node) => {
    const url = resolveRoute(node.dataset.routeTemplate);
    if (!url) return;
    node.setAttribute("hx-get", url);
    if (window.htmx) {
      window.htmx.process(node);
      window.htmx.ajax("GET", url, { target: node, swap: "innerHTML" });
    }
  });
}

function resolveRoute(template) {
  let missing = false;
  const url = template.replace(/\{([^}]+)\}/g, (_match, key) => {
    const value = context[key] || document.querySelector(`[data-context-key="${key}"]`)?.value.trim();
    if (!value) {
      missing = true;
      return "";
    }
    context[key] = value;
    return encodeURIComponent(value);
  });
  if (missing) {
    showToast("Enter cell and asset context first");
    return "";
  }
  return url;
}

function filterTable(input) {
  const scope = input.closest(".card, .content-region, [data-table]") || document;
  const query = input.value.toLowerCase();
  scope.querySelectorAll("tbody tr, .asset-card, .catalog-card, .resource-row").forEach((row) => {
    row.hidden = !row.textContent.toLowerCase().includes(query);
  });
}

function sortTable(button) {
  const table = button.closest("table");
  if (!table) return;
  const headers = Array.from(table.querySelectorAll("[data-sort-key]"));
  const index = headers.indexOf(button);
  const body = table.querySelector("tbody");
  const rows = Array.from(body.querySelectorAll("tr"));
  rows.sort((left, right) => (
    left.children[index].textContent.localeCompare(right.children[index].textContent)
  ));
  body.replaceChildren(...rows);
}

function showToast(message) {
  const toast = document.createElement("div");
  toast.className = "toast";
  toast.textContent = message;
  toastRegion.append(toast);
  window.setTimeout(() => toast.remove(), 2600);
}
