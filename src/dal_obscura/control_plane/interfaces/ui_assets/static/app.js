const apiRoot = document.documentElement.dataset.apiRoot || "/v1";
const state = {
  token: sessionStorage.getItem("dal-obscura-admin-token") || "",
  section: "tenants",
};

const title = document.querySelector("#section-title");
const content = document.querySelector("#content-region");
const tokenForm = document.querySelector("#token-form");
const tokenInput = document.querySelector("#admin-token");
const nav = document.querySelector("#section-nav");
const toasts = document.querySelector("#toast-region");

const sectionConfig = {
  tenants: { list: "/tenants", context: [] },
  cells: { list: "/cells", context: [] },
  assignments: { list: "/cell-tenant-assignments", context: [] },
  runtime: { list: "/cells/{cell_id}/runtime-settings", context: ["cell_id"] },
  catalogs: { list: "/cells/{cell_id}/catalogs", context: ["cell_id"] },
  assets: { list: "/cells/{cell_id}/assets", context: ["cell_id"] },
  policies: { list: "/assets/{asset_id}/policy-rules", context: ["asset_id"] },
  auth: { list: "/cells/{cell_id}/auth-providers", context: ["cell_id"] },
  publications: { list: "/cells/{cell_id}/publications", context: ["cell_id"] },
};

if (state.token) {
  tokenInput.value = state.token;
}

tokenForm.addEventListener("submit", (event) => {
  event.preventDefault();
  state.token = tokenInput.value.trim();
  sessionStorage.setItem("dal-obscura-admin-token", state.token);
  showToast("Token set for this browser session");
  loadSection(state.section);
});

nav.addEventListener("click", (event) => {
  const button = event.target.closest("[data-section]");
  if (!button) return;
  state.section = button.dataset.section;
  document.querySelectorAll(".nav-item").forEach((item) => {
    item.classList.toggle("is-active", item === button);
  });
  title.textContent = button.textContent;
  loadSection(state.section);
});

async function apiGet(path) {
  const resolvedPath = path;
  const response = await fetch(`${apiRoot}${resolvedPath}`, {
    headers: { Authorization: `Bearer ${state.token}` },
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
}

async function loadSection(section) {
  if (!state.token) {
    renderEmpty("Connect to the API", "Enter the control-plane admin token first.");
    return;
  }
  try {
    const path = sectionPath(section);
    if (!path) {
      renderEmpty("Choose context", "This section needs a selected cell or asset.");
      return;
    }
    const data = await apiGet(path);
    renderTable(section, Array.isArray(data) ? data : [data]);
  } catch (error) {
    renderError(error.message);
  }
}

function sectionPath(section) {
  const config = sectionConfig[section];
  if (!config) return null;
  return resolvePath(config.list);
}

function resolvePath(path) {
  return path.replace(/\{([^}]+)\}/g, (_match, key) => {
    const value = window.prompt(`Enter ${key}`);
    if (!value) {
      throw new Error(`Missing ${key}`);
    }
    return encodeURIComponent(value);
  });
}

function renderTable(section, rows) {
  if (rows.length === 0) {
    renderEmpty(`No ${section}`, "No records were returned by the API.");
    return;
  }
  const columns = Object.keys(rows[0]);
  content.innerHTML = `
    <div class="section-toolbar">
      <input class="filter-input" type="search" placeholder="Filter rows" data-filter>
      <button class="primary-action" type="button" data-refresh>Refresh</button>
    </div>
    <div class="table-wrap">
      <table class="resource-table">
        <thead>
          <tr>${columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr>
        </thead>
        <tbody>
          ${rows.map((row) => renderRow(columns, row)).join("")}
        </tbody>
      </table>
    </div>
  `;
  content.querySelector("[data-refresh]").addEventListener("click", () => loadSection(section));
  content.querySelector("[data-filter]").addEventListener("input", (event) => {
    const query = event.target.value.toLowerCase();
    content.querySelectorAll("tbody tr").forEach((row) => {
      row.hidden = !row.textContent.toLowerCase().includes(query);
    });
  });
}

function renderJsonDrawer(titleText, submitLabel, initialValue, onSubmit) {
  const drawer = document.createElement("dialog");
  drawer.className = "drawer";
  drawer.innerHTML = `
    <form method="dialog" class="drawer-form">
      <header class="drawer-header">
        <h2>${escapeHtml(titleText)}</h2>
        <button type="button" data-close>Close</button>
      </header>
      <textarea class="json-field" rows="18">${escapeHtml(JSON.stringify(initialValue, null, 2))}</textarea>
      <footer class="drawer-footer">
        <button class="primary-action" type="submit">${escapeHtml(submitLabel)}</button>
      </footer>
    </form>
  `;
  drawer.querySelector("[data-close]").addEventListener("click", () => drawer.close());
  drawer.querySelector("form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      const payload = JSON.parse(drawer.querySelector(".json-field").value);
      await onSubmit(payload);
      drawer.close();
    } catch (error) {
      showToast(error.message);
    }
  });
  document.body.append(drawer);
  drawer.showModal();
  drawer.addEventListener("close", () => drawer.remove());
}

function renderRow(columns, row) {
  return `<tr>${columns.map((column) => `<td>${escapeHtml(formatValue(row[column]))}</td>`).join("")}</tr>`;
}

function formatValue(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function renderEmpty(heading, message) {
  content.innerHTML = `<div class="empty-state"><h2>${escapeHtml(heading)}</h2><p>${escapeHtml(message)}</p></div>`;
}

function renderError(message) {
  content.innerHTML = `<div class="error-state"><h2>API request failed</h2><p>${escapeHtml(message)}</p></div>`;
}

function showToast(message) {
  const toast = document.createElement("div");
  toast.className = "toast";
  toast.textContent = message;
  toasts.append(toast);
  window.setTimeout(() => toast.remove(), 2500);
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[char]));
}
