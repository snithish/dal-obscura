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
  const response = await fetch(`${apiRoot}${path}`, {
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
  const path = sectionPath(section);
  if (!path) {
    renderEmpty("Choose context", "This section needs a selected cell or asset.");
    return;
  }
  try {
    const data = await apiGet(path);
    renderTable(section, Array.isArray(data) ? data : [data]);
  } catch (error) {
    renderError(error.message);
  }
}

function sectionPath(section) {
  const paths = {
    tenants: "/tenants",
    cells: "/cells",
    assignments: "/cell-tenant-assignments",
  };
  return paths[section] || null;
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
