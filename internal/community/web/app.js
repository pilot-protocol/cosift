"use strict";
const $ = (id) => document.getElementById(id);
let user = null,
  signingUp = true,
  saved = [],
  currentQuery = "",
  currentMode = "search",
  selectedMode = "search",
  selected = new Set(),
  noticeTimer,
  guestUntil = 0;
const topics = [
  "Technology",
  "Science",
  "Design",
  "Open source",
  "Climate",
  "History",
  "Health & wellbeing",
  "Arts & culture",
  "Business",
  "Education",
  "Engineering",
  "Food & travel",
];
function notify(message, error = false) {
  clearTimeout(noticeTimer);
  $("notice").textContent = message;
  $("notice").className = error ? "error" : "";
  $("notice").hidden = false;
  noticeTimer = setTimeout(
    () => ($("notice").hidden = true),
    error ? 10000 : 5500,
  );
}
async function api(path, method = "GET", body) {
  const headers = { "X-Cosift-Client": "community" };
  if (body && !(body instanceof FormData)) {
    headers["Content-Type"] = "application/json";
    body = JSON.stringify(body);
  }
  const response = await fetch("/api/" + path, {
    method,
    headers,
    body,
    credentials: "same-origin",
  });
  let data;
  try {
    data = await response.json();
  } catch {
    throw new Error("The server is unavailable. Please try again.");
  }
  if (!response.ok) {
    if (data.retry_at) {
      guestUntil = data.retry_at;
      renderGuestAllowance();
    }
    if (response.status === 401 && user) {
      user = null;
      showScreen("auth");
    }
    throw new Error(data.error || "Something went wrong. Please try again.");
  }
  return data;
}
function showScreen(id) {
  $("notice").hidden = true;
  for (const name of ["auth", "onboarding", "app"])
    $(name).hidden = name !== id;
}
function el(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
}
function safeLink(raw) {
  try {
    const u = new URL(raw);
    return ["https:", "http:"].includes(u.protocol) ? u.href : null;
  } catch {
    return null;
  }
}
function link(raw, text) {
  const a = el("a", text);
  const href = safeLink(raw);
  if (href) {
    a.href = href;
    a.target = "_blank";
    a.rel = "noopener noreferrer";
  }
  return a;
}
function date(ts) {
  return new Date(ts * 1000).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}
async function busy(form, action) {
  const button = form.querySelector("button[type=submit],button.primary");
  button.disabled = true;
  try {
    await action();
  } catch (e) {
    notify(e.message, true);
  } finally {
    button.disabled = false;
  }
}
$("auth-toggle").onclick = () => {
  signingUp = !signingUp;
  $("name-field").hidden = !signingUp;
  $("auth-form").elements.name.required = signingUp;
  $("auth-form").elements.password.autocomplete = signingUp
    ? "new-password"
    : "current-password";
  $("auth-title").textContent = signingUp
    ? "Make yourself at home."
    : "Welcome back.";
  $("auth-description").textContent = signingUp
    ? "One account for your searches and contributions."
    : "Pick up where your curiosity left off.";
  $("auth-submit").textContent = signingUp ? "Create account ↗" : "Sign in ↗";
  $("auth-switch-copy").textContent = signingUp
    ? "Already have an account?"
    : "New to Cosift?";
  $("auth-toggle").textContent = signingUp ? "Sign in" : "Create an account";
};
$("auth-form").onsubmit = (event) => {
  event.preventDefault();
  busy(event.target, async () => {
    const form = new FormData(event.target);
    user = await api(signingUp ? "register" : "login", "POST", {
      name: form.get("name"),
      email: form.get("email"),
      password: form.get("password"),
    });
    event.target.reset();
    await enter();
  });
};
function onboarding() {
  selected = new Set(user.interests.filter((v) => topics.includes(v)));
  $("custom-interests").value = user.interests
    .filter((v) => !topics.includes(v))
    .join(", ");
  $("topics").replaceChildren();
  for (const topic of topics) {
    const button = el("button", topic, "topic");
    button.type = "button";
    button.setAttribute("aria-pressed", selected.has(topic));
    button.onclick = () => {
      if (selected.has(topic)) selected.delete(topic);
      else selected.add(topic);
      button.setAttribute("aria-pressed", selected.has(topic));
    };
    $("topics").append(button);
  }
  $("skip-interests").textContent = user.onboarded ? "Cancel" : "Skip for now";
  showScreen("onboarding");
}
$("interests-form").onsubmit = (event) => {
  event.preventDefault();
  busy(event.target, async () => {
    const interests = [
      ...selected,
      ...$("custom-interests")
        .value.split(",")
        .map((v) => v.trim())
        .filter(Boolean),
    ];
    user = await api("interests", "PUT", { interests });
    await enter();
    notify("Your interests are saved.");
  });
};
$("skip-interests").onclick = async () => {
  try {
    if (!user.onboarded)
      user = await api("interests", "PUT", { interests: [] });
    await enter();
  } catch (e) {
    notify(e.message, true);
  }
};
async function refreshCredits() {
  $("credit-balance").hidden = !user;
  if (user) {
    const c = await api("credits");
    $("credit-balance").textContent =
      `${c.balance} credits · 1 per extra request`;
  }
}
async function enter() {
  await refreshCredits();
  if (!currentQuery) {
    $("results").replaceChildren();
    $("search-heading").hidden = true;
    $("search-empty").hidden = false;
  }
  $("guest-banner").hidden = !!user;
  if (!user) {
    saved = [];
    $("saved-count").textContent = "0";
    $("account-name").textContent = "Guest";
    $("avatar").textContent = "G";
    $("edit-interests").textContent = "Create an account";
    $("logout").setAttribute("aria-label", "Sign in");
    $("logout").title = "Sign in";
    showScreen("app");
    suggestions();
    await refreshGuest();
    view("search");
    updateSaveButton();
    return;
  }
  $("edit-interests").textContent = "Edit interests";
  $("logout").setAttribute("aria-label", "Sign out");
  $("logout").title = "Sign out";
  if (!user.onboarded) {
    onboarding();
    return;
  }
  $("account-name").textContent = user.name;
  $("avatar").textContent = user.name.slice(0, 1).toUpperCase();
  showScreen("app");
  suggestions();
  await refreshSaved();
  view("search");
}
function suggestions() {
  $("suggestions").replaceChildren(
    el("span", user?.interests.length ? "YOUR INTERESTS" : "TRY A TOPIC"),
  );
  for (const topic of (user?.interests.length
    ? user.interests
    : ["Open source", "Climate", "Design"]
  ).slice(0, 6)) {
    const button = el("button", topic);
    button.onclick = () => runSearch(topic);
    $("suggestions").append(button);
  }
}
async function view(name) {
  for (const value of ["search", "saved", "contribute"])
    $("view-" + value).hidden = value !== name;
  document.querySelectorAll("nav [data-view]").forEach((button) => {
    button.classList.toggle("active", button.dataset.view === name);
    if (button.dataset.view === name)
      button.setAttribute("aria-current", "page");
    else button.removeAttribute("aria-current");
  });
  try {
    if (name === "saved") {
      if (user) await refreshSaved();
      else {
        $("saved-list").replaceChildren(
          el(
            "div",
            "Create an account to save searches and return to them anytime.",
            "empty-state",
          ),
        );
      }
    }
    if (name === "contribute") await refreshContributions();
  } catch (e) {
    notify(e.message, true);
  }
}
document
  .querySelectorAll("[data-view]")
  .forEach((button) => (button.onclick = () => view(button.dataset.view)));
$("edit-interests").onclick = () => (user ? onboarding() : showScreen("auth"));
$("guest-signup").onclick = () => showScreen("auth");
$("continue-guest").onclick = () =>
  enter().catch((e) => notify(e.message, true));
async function refreshGuest() {
  if (user) return;
  const status = await api("guest");
  guestUntil = status.retry_at;
  renderGuestAllowance();
}
function renderGuestAllowance() {
  if (user) return;
  const seconds = Math.max(0, guestUntil - Math.floor(Date.now() / 1000));
  $("guest-allowance").textContent = seconds
    ? "Your next request is available in " +
      Math.floor(seconds / 60) +
      "m " +
      String(seconds % 60).padStart(2, "0") +
      "s."
    : "One request available across Search, Research, Answer, and contributions.";
}
setInterval(renderGuestAllowance, 1000);
$("logout").onclick = async () => {
  if (!user) {
    showScreen("auth");
    return;
  }
  try {
    searchSequence++;
    await api("logout", "POST", {});
    user = null;
    saved = [];
    currentQuery = "";
    $("results").replaceChildren();
    $("search-heading").hidden = true;
    $("search-empty").hidden = false;
    $("query").value = "";
    await enter();
  } catch (e) {
    notify(e.message, true);
  }
};
let searchSequence = 0;
const modeLabels = { search: "Search", research: "Research", answer: "Answer" };
function selectMode(mode) {
  selectedMode = modeLabels[mode] ? mode : "search";
  document
    .querySelectorAll("[data-mode]")
    .forEach((b) =>
      b.setAttribute("aria-pressed", b.dataset.mode === selectedMode),
    );
  $("mode-description").textContent = {
    search: "Find webpages in the Cosift index.",
    research: "Explore a question with multi-step research and cited sources.",
    answer: "Get a direct, grounded answer with sources.",
  }[selectedMode];
  $("query").placeholder =
    selectedMode === "search"
      ? "A question, an idea, a rabbit hole…"
      : "What would you like to understand?";
  if (!$("search-form").querySelector("button").disabled)
    $("search-form").querySelector("button").textContent =
      modeLabels[selectedMode] + " ↗";
}
document
  .querySelectorAll("[data-mode]")
  .forEach((b) => (b.onclick = () => selectMode(b.dataset.mode)));
async function runSearch(q, mode = selectedMode) {
  selectMode(mode);
  q = q.trim();
  if (!q) return;
  const sequence = ++searchSequence;
  view("search");
  $("query").value = q;
  const button = $("search-form").querySelector("button");
  button.disabled = true;
  button.textContent = {
    search: "Searching…",
    research: "Researching…",
    answer: "Answering…",
  }[mode];
  $("search-heading").hidden = true;
  $("search-empty").hidden = true;
  $("results").replaceChildren(
    el(
      "p",
      mode === "search"
        ? "Searching the Cosift index…"
        : mode === "research"
          ? "Cosift is researching your question. This can take a few minutes…"
          : "Cosift is gathering sources and writing your answer…",
      "muted",
    ),
  );
  currentQuery = "";
  try {
    const data = await api(mode + "?q=" + encodeURIComponent(q));
    if (sequence !== searchSequence) return;
    currentQuery = q;
    currentMode = mode;
    const hits = Array.isArray(data.hits) ? data.hits : [];
    $("results").replaceChildren();
    $("search-heading").hidden = false;
    $("results-title").textContent =
      mode !== "search"
        ? modeLabels[mode]
        : hits.length
          ? hits.length + " results to explore"
          : "No results yet";
    updateSaveButton();
    if (mode !== "search") renderSynthesis(data, mode);
    if (mode === "search" && !hits.length)
      $("results").append(
        el(
          "div",
          "Try a broader phrase or contribute a useful source to help this part of the index grow.",
          "empty-state",
        ),
      );
    for (const hit of mode === "search" ? hits : []) {
      const article = el("article", undefined, "result");
      let host = hit.url || "";
      try {
        host = new URL(host).hostname;
      } catch {}
      article.append(el("div", host, "domain"));
      const heading = el("h3");
      heading.append(link(hit.url, hit.title || hit.url || "Untitled webpage"));
      article.append(heading);
      const snippet =
        hit.excerpt || hit.snippet || hit.text || hit.description || "";
      article.append(el("p", String(snippet).slice(0, 400)));
      $("results").append(article);
    }
    if (!user) await refreshGuest(); else await refreshCredits();
  } catch (e) {
    if (sequence === searchSequence) {
      $("results").replaceChildren(el("div", e.message, "empty-state"));
      notify(e.message, true);
    }
  } finally {
    if (sequence === searchSequence) {
      button.disabled = false;
      button.textContent = modeLabels[selectedMode] + " ↗";
    }
  }
}
$("search-form").onsubmit = (event) => {
  event.preventDefault();
  runSearch($("query").value);
};
function updateSaveButton() {
  if (!user) {
    $("save-search").textContent = "Sign in to save";
    $("save-search").disabled = !currentQuery;
    return;
  }
  const exists = saved.some(
    (v) => v.query === currentQuery && (v.mode || "search") === currentMode,
  );
  $("save-search").textContent = exists ? "✓ Request saved" : "＋ Save request";
  $("save-search").disabled = exists || !currentQuery;
}
$("save-search").onclick = async () => {
  if (!user) {
    showScreen("auth");
    return;
  }
  $("save-search").disabled = true;
  try {
    await api("saved", "POST", { query: currentQuery, mode: currentMode });
    await refreshSaved();
    notify(modeLabels[currentMode] + " request saved.");
  } catch (e) {
    notify(e.message, true);
  } finally {
    updateSaveButton();
  }
};
async function refreshSaved() {
  saved = await api("saved");
  $("saved-count").textContent = saved.length;
  $("saved-list").replaceChildren();
  updateSaveButton();
  if (!saved.length)
    $("saved-list").append(
      el(
        "div",
        "Nothing saved just yet. Search for something interesting, then choose “Save search”.",
        "empty-state",
      ),
    );
  for (const item of saved) {
    const card = el("article", undefined, "saved-card");
    const left = el("div");
    const run = el("button", item.query + " ↗", "text-button run-saved");
    run.onclick = () => runSearch(item.query, item.mode || "search");
    left.append(
      run,
      el(
        "p",
        modeLabels[item.mode || "search"] + " · Saved " + date(item.created_at),
        "fine",
      ),
    );
    const remove = el("button", "Remove", "text-button");
    remove.setAttribute("aria-label", "Remove saved search " + item.query);
    remove.onclick = async () => {
      remove.disabled = true;
      try {
        await api("saved/" + encodeURIComponent(item.id), "DELETE");
        await refreshSaved();
      } catch (e) {
        notify(e.message, true);
        remove.disabled = false;
      }
    };
    card.append(left, remove);
    $("saved-list").append(card);
  }
}
$("contribution-form").onsubmit = (event) => {
  event.preventDefault();
  busy(event.target, async () => {
    const text = $("urls").value.trim(),
      file = $("csv").files[0];
    if (text && file)
      throw new Error("Use either the text field or a CSV, not both.");
    if (!text && !file)
      throw new Error("Add at least one webpage URL or choose a CSV.");
    let body;
    if (file) {
      if (file.size > 1000000)
        throw new Error("Choose a CSV smaller than 1 MB.");
      body = new FormData();
      body.append("file", file);
    } else {
      body = {
        urls: text
          .split(/\r?\n/)
          .map((v) => v.trim())
          .filter(Boolean),
      };
      if (body.urls.length > 100)
        throw new Error("Submit up to 100 URLs at a time.");
    }
    const data = await api("submissions", "POST", body);
    event.target.reset();
    notify(
      data.accepted +
        " webpage" +
        (data.accepted === 1 ? "" : "s") +
        " saved for safety checks." +
        (data.duplicates
          ? " " + data.duplicates + " already contributed."
          : ""),
    );
    await refreshContributions();
    if (!user) await refreshGuest(); else await refreshCredits();
  });
};
async function refreshContributions() {
  await refreshCredits();
  if (!user) {
    $("contribution-list").replaceChildren(
      el(
        "div",
        "Sign in to keep a personal contribution history. Guest submissions are saved for crawling without an account.",
        "empty-state",
      ),
    );
    return;
  }
  const items = await api("submissions");
  $("contribution-list").replaceChildren();
  if (!items.length)
    $("contribution-list").append(
      el(
        "div",
        "Your next great find can be your first contribution.",
        "empty-state",
      ),
    );
  for (const item of items) {
    const row = el("div", undefined, "submission-row"),
      left = el("div");
    left.append(
      ["queued", "indexed"].includes(item.status)
        ? link(item.url, item.url)
        : el("span", item.url, "submitted-url"),
      el("p", date(item.created_at), "fine"),
    );
    if (item.reason) left.append(el("p", item.reason, "fine"));
    row.append(
      left,
      el(
        "span",
        {
          queued: "Queued",
          indexed: "Indexed",
          pending: "Checking",
          rejected: "Rejected",
          unverified: "Unverified",
        }[item.status] || "Checking",
        "status " +
          (["queued", "indexed", "rejected", "unverified"].includes(item.status)
            ? item.status
            : "pending"),
      ),
    );
    $("contribution-list").append(row);
  }
}
$("refresh-contributions").onclick = () =>
  refreshContributions().catch((e) => notify(e.message, true));
(async () => {
  try {
    user = await api("me");
  } catch {
    user = null;
  }
  try {
    await enter();
  } catch (e) {
    showScreen("auth");
    notify(e.message, true);
  }
})();

function renderSynthesis(data, mode) {
  if (Array.isArray(data.plan) && data.plan.length) {
    const details = el("details", undefined, "research-plan"),
      summary = el(
        "summary",
        "Research plan · " + data.plan.length + " questions",
      ),
      list = el("ol");
    for (const step of data.plan) list.append(el("li", String(step)));
    details.append(summary, list);
    $("results").append(details);
  }
  const sources = Array.isArray(data.sources) ? data.sources : [],
    ids = new Set(sources.map((s, i) => String(s.id || i + 1)));
  const article = el("article", undefined, "synthesis");
  function inline(parent, text) {
    const pattern = /(\*\*([^*]+)\*\*)|(`([^`]+)`)|(\[(\d+(?:\s*,\s*\d+)*)\])/g;
    let end = 0;
    for (const m of text.matchAll(pattern)) {
      parent.append(document.createTextNode(text.slice(end, m.index)));
      if (m[2]) parent.append(el("strong", m[2]));
      else if (m[4]) parent.append(el("code", m[4]));
      else {
        for (const id of m[6].split(",").map((v) => v.trim())) {
          const a = el("a", "[" + id + "]", "citation");
          if (ids.has(id)) a.href = "#source-" + id;
          parent.append(a);
        }
      }
      end = m.index + m[0].length;
    }
    parent.append(document.createTextNode(text.slice(end)));
  }
  const answer = String(
    data.answer || "Cosift did not return an answer for this request.",
  );
  let list = null,
    code = null;
  for (const line of answer.split(/\r?\n/)) {
    if (line.trim().startsWith("```")) {
      if (code) {
        code = null;
      } else {
        const pre = el("pre");
        code = el("code", "");
        pre.append(code);
        article.append(pre);
      }
      list = null;
      continue;
    }
    if (code) {
      code.textContent += line + "\n";
      continue;
    }
    if (!line.trim()) {
      list = null;
      continue;
    }
    const heading = line.match(/^#{1,6}\s+(.+)$/),
      bullet = line.match(/^\s*(?:[-*]|\d+\.)\s+(.+)$/);
    if (heading) {
      list = null;
      const h = el("h3");
      inline(h, heading[1]);
      article.append(h);
    } else if (bullet) {
      if (!list) {
        list = el("ul");
        article.append(list);
      }
      const li = el("li");
      inline(li, bullet[1]);
      list.append(li);
    } else {
      list = null;
      const p = el("p");
      inline(p, line);
      article.append(p);
    }
  }
  $("results").append(article);
  if (sources.length) {
    $("results").append(el("h3", "Sources", "sources-heading"));
    for (const [i, source] of sources.entries()) {
      const card = el("article", undefined, "result source-card");
      card.id = "source-" + (source.id || i + 1);
      const h = el("h3");
      h.append(
        link(
          source.url,
          "[" +
            (source.id || i + 1) +
            "] " +
            (source.title || source.url || "Source"),
        ),
      );
      card.append(h, el("p", source.excerpt || source.url || ""));
      $("results").append(card);
    }
  }
  if (Array.isArray(data.warnings) && data.warnings.length)
    $("results").append(el("p", data.warnings.join(" "), "muted"));
  if (data.model || data.took)
    $("results").append(
      el("p", [data.model, data.took].filter(Boolean).join(" · "), "fine"),
    );
}
