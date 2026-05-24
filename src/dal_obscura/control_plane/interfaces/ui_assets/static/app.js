const tokenKey = "dal-obscura-admin-token";
const tokenForm = document.querySelector("#token-form");
const tokenInput = document.querySelector("#admin-token");
const toastRegion = document.querySelector("#toast-region");
const context = {};
let lastToast = "";

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

document.addEventListener("submit", (event) => {
  const form = event.target.closest("[data-put-template]");
  if (!form) return;
  const url = resolveRoute(form.dataset.putTemplate, { form, notify: true });
  if (!url) {
    event.preventDefault();
    return;
  }
  form.setAttribute("hx-put", url);
  if (window.htmx) window.htmx.process(form);
}, true);

document.addEventListener("click", (event) => {
  const panelButton = event.target.closest("[data-panel]");
  if (panelButton) {
    if (panelButton.matches("[data-requires-context]") && !hasTenantCellContext()) {
      showPanel("global-admin");
      showToast("Select a tenant and cell to unlock configuration");
      return;
    }
    showPanel(panelButton.dataset.panel);
    return;
  }
  const contextButton = event.target.closest("[data-set-context]");
  if (contextButton) {
    setContextValue(
      contextButton.dataset.setContext,
      contextButton.dataset.contextValue,
      contextButton.dataset.contextLabel,
    );
    return;
  }
  const refresh = event.target.closest("[data-refresh-context]");
  if (refresh) {
    if (!hasTenantCellContext()) {
      showToast("Select a tenant and cell first");
      return;
    }
    refreshContextRoutes({ notify: true });
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
  updateContextLocks();
  refreshContextRoutes();
}

function refreshContextRoutes(options = {}) {
  const activePanel = document.querySelector(".panel.is-active");
  if (!activePanel) return;
  activePanel.querySelectorAll(":scope > [data-route-template]").forEach((node) => {
    const url = resolveRoute(node.dataset.routeTemplate, options);
    if (!url) return;
    node.setAttribute("hx-get", url);
    if (window.htmx) {
      window.htmx.ajax("GET", url, { target: node, swap: "innerHTML" });
    }
  });
}

function resolveRoute(template, options = {}) {
  let missing = false;
  const url = template.replace(/\{([^}]+)\}/g, (_match, key) => {
    const formValue = options.form?.querySelector(`[name="${key}"]`)?.value.trim();
    const value = context[key]
      || formValue
      || document.querySelector(`[data-context-key="${key}"]`)?.value.trim();
    if (!value) {
      missing = true;
      return "";
    }
    context[key] = value;
    return encodeURIComponent(value);
  });
  if (missing && options.notify) {
    showToast("Select the required setup context first");
    return "";
  }
  return url;
}

function setContextValue(key, value, label) {
  if (!key || !value) return;
  context[key] = value;
  if (key === "tenant_id") {
    delete context.cell_id;
    document.querySelectorAll('[data-context-key="cell_id"]').forEach((input) => {
      input.value = "";
    });
    document.querySelectorAll('[data-context-summary="cell_id"]').forEach((node) => {
      node.textContent = "No cell selected";
    });
  }
  document.querySelectorAll(`[data-context-key="${key}"]`).forEach((input) => {
    input.value = value;
  });
  document.querySelectorAll(`[data-context-summary="${key}"]`).forEach((node) => {
    node.textContent = label || value;
  });
  updateContextLocks();
  if (key === "tenant_id") {
    loadTenantCells();
  }
  showToast(`${key === "tenant_id" ? "Tenant" : "Cell"} selected`);
}

function hasTenantCellContext() {
  return Boolean(
    context.tenant_id
    || document.querySelector('[data-context-key="tenant_id"]')?.value.trim()
  ) && Boolean(
    context.cell_id
    || document.querySelector('[data-context-key="cell_id"]')?.value.trim()
  );
}

function updateContextLocks() {
  const unlocked = hasTenantCellContext();
  document.querySelectorAll("[data-requires-context]").forEach((button) => {
    button.classList.toggle("is-locked", !unlocked);
    button.setAttribute("aria-disabled", unlocked ? "false" : "true");
    const lock = button.querySelector(".lock-icon");
    if (lock) lock.hidden = unlocked;
  });
  document.querySelectorAll("[data-context-lock]").forEach((panel) => {
    panel.hidden = unlocked;
  });
  document.querySelectorAll("[data-needs-tenant]").forEach((panel) => {
    panel.classList.toggle("is-disabled", !Boolean(context.tenant_id));
  });
  const cellForm = document.querySelector("#cell-form");
  if (cellForm) {
    const url = context.tenant_id ? resolveRoute(cellForm.dataset.postTemplate) : "";
    if (url) {
      cellForm.setAttribute("hx-post", url);
      if (window.htmx) window.htmx.process(cellForm);
    } else {
      cellForm.removeAttribute("hx-post");
    }
    cellForm.querySelectorAll("input, button").forEach((field) => {
      field.disabled = !Boolean(context.tenant_id);
    });
  }
  const assignmentForm = document.querySelector("#cell-assignment-form");
  if (assignmentForm) {
    const url = context.tenant_id ? resolveRoute(assignmentForm.dataset.postTemplate) : "";
    if (url) {
      assignmentForm.setAttribute("hx-post", url);
      if (window.htmx) window.htmx.process(assignmentForm);
    } else {
      assignmentForm.removeAttribute("hx-post");
    }
    assignmentForm.querySelectorAll("input, button").forEach((field) => {
      field.disabled = !Boolean(context.tenant_id);
    });
  }
  document.querySelectorAll(".setup-step").forEach((step) => {
    const stepName = step.dataset.step;
    step.classList.toggle(
      "is-active",
      (stepName === "tenant" && !context.tenant_id)
      || (stepName === "cell" && context.tenant_id && !context.cell_id)
      || (stepName === "workspace" && unlocked),
    );
    step.classList.toggle("is-complete", stepName === "tenant" ? Boolean(context.tenant_id) : stepName === "cell" ? Boolean(context.cell_id) : unlocked);
  });
  document.querySelectorAll("[data-set-context]").forEach((button) => {
    const key = button.dataset.setContext;
    button.classList.toggle("is-selected", Boolean(key && context[key] === button.dataset.contextValue));
  });
}

function loadTenantCells() {
  const container = document.querySelector("#cells-list");
  if (!container || !context.tenant_id) return;
  const url = resolveRoute(container.dataset.listTemplate);
  if (!url) return;
  container.setAttribute("hx-get", url);
  if (window.htmx) {
    window.htmx.ajax("GET", url, { target: container, swap: "innerHTML" });
  }
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
  if (message === lastToast) return;
  lastToast = message;
  const toast = document.createElement("div");
  toast.className = "toast";
  toast.textContent = message;
  toastRegion.append(toast);
  window.setTimeout(() => {
    toast.remove();
    if (lastToast === message) lastToast = "";
  }, 2200);
}

updateContextLocks();
