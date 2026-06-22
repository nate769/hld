// Typeahead frontend.
//
// Drives the search box: debounced /suggest calls, keyboard nav of the
// dropdown, form submit -> POST /search, and a trending sidebar that
// refreshes after each search. All requests hit the LB on the same origin.

const DEBOUNCE_MS = 150;
const MAX_PREFIX_LEN = 100;

const el = {
  input: document.getElementById("q"),
  suggestions: document.getElementById("suggestions"),
  trending: document.getElementById("trending"),
  meta: document.getElementById("meta"),
  form: document.getElementById("search-form"),
  lucky: document.getElementById("lucky-btn"),
};

const state = {
  active: -1,
  items: [],
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function debounce(fn, wait) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), wait);
  };
}

const HTML_ESCAPES = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };
function escapeHtml(s) {
  return s.replace(/[&<>"']/g, (c) => HTML_ESCAPES[c]);
}

// Bold the matched prefix. Slice the raw text on character boundaries first,
// then escape — escaping first and slicing by raw length would split entities.
function highlightPrefix(text, prefix) {
  if (prefix && text.toLowerCase().startsWith(prefix.toLowerCase())) {
    const head = escapeHtml(text.slice(0, prefix.length));
    const tail = escapeHtml(text.slice(prefix.length));
    return `<b>${head}</b>${tail}`;
  }
  return escapeHtml(text);
}

async function fetchJson(url, init) {
  const res = await fetch(url, init);
  return res.json();
}

// ---------------------------------------------------------------------------
// Suggestions
// ---------------------------------------------------------------------------

function renderSuggestions(prefix, suggestions, shard) {
  state.items = suggestions;
  state.active = -1;

  el.suggestions.innerHTML = suggestions
    .map(
      (s, i) =>
        `<li role="option" data-i="${i}" data-q="${escapeHtml(s)}">` +
        `<span>${highlightPrefix(s, prefix)}</span></li>`,
    )
    .join("");

  if (suggestions.length) {
    el.meta.textContent = `${suggestions.length} suggestions · served by shard ${shard ?? "?"}`;
  } else {
    el.meta.textContent = prefix ? "no suggestions" : "";
  }
}

const requestSuggestions = debounce(async (prefix) => {
  if (!prefix) {
    renderSuggestions("", []);
    return;
  }
  try {
    const data = await fetchJson(`/suggest?q=${encodeURIComponent(prefix)}`);
    // Ignore stale responses if the input has moved on.
    if (el.input.value.trim() === prefix) {
      renderSuggestions(prefix, data.suggestions ?? [], data.shard);
    }
  } catch {
    el.meta.textContent = "suggest request failed";
  }
}, DEBOUNCE_MS);

function highlightActive() {
  const items = el.suggestions.querySelectorAll("li");
  items.forEach((li, i) => li.setAttribute("aria-selected", i === state.active));
}

function moveActive(delta) {
  const n = state.items.length;
  if (n === 0) return;
  state.active = (state.active + delta + n) % n;
  highlightActive();
}

// ---------------------------------------------------------------------------
// Search submission
// ---------------------------------------------------------------------------

async function submitSearch(query) {
  const q = query.trim();
  if (!q) return;
  el.input.value = q;
  renderSuggestions(q, []);
  el.meta.textContent = `searched "${q}"`;
  try {
    await fetch("/search", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query: q }),
    });
  } catch {
    // Fire-and-forget; the server buffers asynchronously.
  }
  loadTrending();
}

// ---------------------------------------------------------------------------
// Trending sidebar
// ---------------------------------------------------------------------------

async function loadTrending() {
  try {
    const data = await fetchJson("/trending");
    const items = data.trending ?? [];
    el.trending.innerHTML = items
      .map(
        (t, i) =>
          `<li><span class="rank">${i + 1}.</span>` +
          `<a data-q="${escapeHtml(t.query)}">${escapeHtml(t.query)}</a></li>`,
      )
      .join("");
  } catch {
    el.trending.innerHTML = "";
  }
}

// ---------------------------------------------------------------------------
// Event wiring
// ---------------------------------------------------------------------------

el.input.addEventListener("input", () => {
  const value = el.input.value.trim().slice(0, MAX_PREFIX_LEN);
  requestSuggestions(value);
});

el.input.addEventListener("keydown", (e) => {
  if (e.key === "ArrowDown") {
    e.preventDefault();
    moveActive(1);
  } else if (e.key === "ArrowUp") {
    e.preventDefault();
    moveActive(-1);
  } else if (e.key === "Escape") {
    renderSuggestions("", []);
  }
});

el.form.addEventListener("submit", (e) => {
  e.preventDefault();
  const chosen = state.active >= 0 ? state.items[state.active] : el.input.value;
  submitSearch(chosen);
});

el.lucky.addEventListener("click", () => {
  const chosen = state.items[0] ?? el.input.value;
  submitSearch(chosen);
});

el.suggestions.addEventListener("click", (e) => {
  const li = e.target.closest("li");
  if (li) submitSearch(li.dataset.q);
});

el.suggestions.addEventListener("mouseover", (e) => {
  const li = e.target.closest("li");
  if (!li) return;
  state.active = Number(li.dataset.i);
  highlightActive();
});

el.trending.addEventListener("click", (e) => {
  const link = e.target.closest("a[data-q]");
  if (!link) return;
  el.input.value = link.dataset.q;
  el.input.focus();
  requestSuggestions(link.dataset.q);
});

document.addEventListener("click", (e) => {
  if (!el.input.contains(e.target) && !el.suggestions.contains(e.target)) {
    renderSuggestions("", []);
  }
});

// Initial state
el.input.focus();
loadTrending();
