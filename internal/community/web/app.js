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
let accountGeneration = 0;
const pendingRequests = new Set();
let searchRequest;
let authBusy = false;
let sharedAuth = false, authChallenge = null, authConfigured = false;
let supportsPassword = false, sharedLoginMode = "otp";
function resetAccount(nextUser = null) {
  accountGeneration++;
  for (const controller of pendingRequests) controller.abort();
  pendingRequests.clear();
  searchSequence++;
  user = nextUser;
  saved = [];
  currentQuery = "";
  currentMode = "search";
  selected = new Set();
  checkoutKey = undefined;
  subscriptionCheckoutKey = undefined;
  guestUntil = 0;
  for (const id of ["results", "saved-list", "contribution-list", "topics", "suggestions", "shared-list", "shared-result"])
    $(id).replaceChildren();
  for (const id of ["query", "urls", "csv", "custom-interests", "shared-topic"]) $(id).value = "";
  $("saved-count").textContent = "0";
  $("credit-balance").textContent = "";
  $("credit-balance").hidden = true;
  $("monthly-credits").hidden = true;
  $("buy-credits").hidden = true;
  $("buy-credits").disabled = false;
  $("subscribe-credits").hidden = true;
  $("subscribe-credits").disabled = false;
  $("manage-subscription").hidden = true;
  $("manage-subscription").disabled = false;
  $("payment-info").hidden = true;
  $("search-heading").hidden = true;
  $("search-empty").hidden = false;
  $("search-form").querySelector("button").disabled = false;
  selectMode("search");
  updateSaveButton();
}
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
  if (!message) return;
  clearTimeout(noticeTimer);
  $("notice").textContent = message;
  $("notice").className = error ? "error" : "";
  $("notice").hidden = false;
  noticeTimer = setTimeout(
    () => ($("notice").hidden = true),
    error ? 10000 : 5500,
  );
}
async function api(path, method = "GET", body, controller = new AbortController()) {
  const generation = accountGeneration;
  let timedOut = false;
  const timeout = setTimeout(() => { timedOut = true; controller.abort(); },
    path.startsWith("research") ? 180000 : path.startsWith("answer") ? 90000 : 30000);
  pendingRequests.add(controller);
  try {
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
      signal: controller.signal,
    });
    let data;
    try {
      data = await response.json();
    } catch {
      const error = new Error("The server is unavailable. Please try again.");
      error.status = response.status;
      throw error;
    }
    if (generation !== accountGeneration || controller.signal.aborted)
      throw new DOMException("", "AbortError");
    if (!response.ok) {
      if (data?.retry_at && !data.mode && !user) {
        guestUntil = data.retry_at;
        renderGuestAllowance();
      }
      if (response.status === 401 && user) {
        resetAccount();
        showScreen("auth");
      }
      const error = new Error(data?.error || "Something went wrong. Please try again.");
      error.status = response.status;
      error.retryAfterSeconds = Number(response.headers?.get("Retry-After")) || 0;
      throw error;
    }
    if (data === null || typeof data !== "object") throw new Error("Cosift returned an unexpected response. Please try again.");
    return data;
  } catch (error) {
    // A superseded account/request must not render data or errors in the next view.
    if (generation !== accountGeneration)
      throw new DOMException("", "AbortError");
    if (timedOut) throw new Error("The request timed out. Please try again; your input is still here.");
    if (controller.signal.aborted) throw new DOMException("", "AbortError");
    if (error instanceof TypeError) throw new Error("Could not connect to Cosift. Check your connection and try again.");
    throw error;
  } finally {
    clearTimeout(timeout);
    pendingRequests.delete(controller);
  }
}
function showScreen(id) {
  $("notice").hidden = true;
  for (const name of ["boot", "auth", "onboarding", "app"])
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
  if (sharedAuth) return;
  signingUp = !signingUp;
  $("name-field").hidden = !signingUp;
  $("auth-form").elements.name.required = signingUp;
  $("auth-form").elements.password.autocomplete = signingUp
    ? "new-password"
    : "current-password";
  $("auth-title").textContent = signingUp
    ? "Create your account"
    : "Welcome back.";
  $("auth-description").textContent = signingUp
    ? "One account for your searches and contributions."
    : "Sign in to access your saved requests and contributions.";
  $("auth-submit").textContent = signingUp ? "Create account ↗" : "Sign in ↗";
  $("auth-switch-copy").textContent = signingUp
    ? "Already have an account?"
    : "New to Cosift?";
  $("auth-toggle").textContent = signingUp ? "Sign in" : "Create an account";
};
$("auth-form").onsubmit = (event) => {
  event.preventDefault();
  if (authBusy || !authConfigured) return;
  authBusy = true;
  return busy(event.target, async () => {
    resetAccount();
    const form = new FormData(event.target);
    if (sharedAuth) {
      if (sharedLoginMode === "password" && supportsPassword) {
        user = await api("auth/password", "POST", {email: form.get("email"), password: form.get("password")});
        sharedLoginMode = "otp";
        event.target.reset();
        renderSharedAuth();
        await enterAfterLogin();
        return;
      }
      if (!authChallenge) {
        authChallenge = await api("auth/start", "POST", {email: form.get("email")});
        renderSharedAuth();
        notify("If the address is eligible, a verification code is on its way. Check your email.");
        return;
      }
      const verification = {request_id: authChallenge.request_id, code: form.get("code")};
      if (sharedLoginMode === "setup" && supportsPassword) verification.password = form.get("password");
      user = await api("auth/verify", "POST", verification);
      authChallenge = null;
      sharedLoginMode = "otp";
      event.target.reset();
      renderSharedAuth();
      await enterAfterLogin();
      return;
    }
    user = await api(signingUp ? "register" : "login", "POST", {
      name: form.get("name"),
      email: form.get("email"),
      password: form.get("password"),
    });
    event.target.reset();
    await enter();
  }).finally(() => { authBusy = false; });
};
function renderSharedAuth() {
  const form = $("auth-form"), checkingCode = !!authChallenge;
  const passwordLogin = sharedLoginMode === "password", settingPassword = sharedLoginMode === "setup";
  $("name-field").hidden = true;
  form.elements.name.required = false;
  $("auth-switch").hidden = true;
  $("password-field").hidden = !(passwordLogin || settingPassword && checkingCode);
  form.elements.password.required = !$("password-field").hidden;
  form.elements.password.autocomplete = settingPassword ? "new-password" : "current-password";
  form.elements.password.placeholder = settingPassword ? "Choose a password (at least 12 characters)" : "Your password";
  $("code-field").hidden = !checkingCode;
  form.elements.code.required = checkingCode;
  form.elements.email.readOnly = checkingCode;
  $("auth-restart").hidden = !checkingCode;
  $("auth-password-switch").hidden = !supportsPassword;
  $("auth-password-switch").textContent = sharedLoginMode === "otp" ? "Use email & password" : "Use an email code instead";
  $("auth-password-reset").hidden = !supportsPassword || !passwordLogin;
  $("auth-title").textContent = settingPassword ? "Set your password" : "Sign in to Cosift";
  $("auth-description").textContent = settingPassword ? "Verify your email to set or reset your password. Your account and credits stay the same."
    : passwordLogin ? "Use the password you set for your Cosift account." : "We’ll email you a sign-in code. No password needed.";
  $("auth-submit").textContent = passwordLogin ? "Sign in →" : checkingCode ? settingPassword ? "Set password and sign in →" : "Verify and sign in →" : "Email me a code →";
}
function selectSharedLogin(mode) {
  if (authBusy || !sharedAuth || !supportsPassword) return;
  sharedLoginMode = mode;
  authChallenge = null;
  $("auth-form").elements.password.value = "";
  $("auth-form").elements.code.value = "";
  renderSharedAuth();
}
$("auth-password-switch").onclick = () => selectSharedLogin(sharedLoginMode === "otp" ? "password" : "otp");
$("auth-password-reset").onclick = () => selectSharedLogin("setup");
async function enterAfterLogin() {
  showScreen("boot");
  try { await enter(); }
  catch (e) {
    if (user) {
      $("boot-message").textContent = "You’re signed in, but your workspace couldn’t load. Please try again.";
      $("boot-spinner").hidden = true;
      $("boot-retry").hidden = false;
    }
    throw e;
  }
}
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
  $("monthly-credits").hidden = true;
  $("buy-credits").hidden = true;
  $("subscribe-credits").hidden = true;
  $("manage-subscription").hidden = true;
  $("payment-info").hidden = true;
  if (user) {
    const c = await api("credits");
    $("credit-balance").textContent =
      `${Number(c.balance).toLocaleString()} credits available`;
    $("billing-balance").textContent = Number(c.balance).toLocaleString();
    $("billing-free").textContent = Number(c.monthly_free_credits || 1000).toLocaleString();
    $("billing-mode-status").textContent = c.payments_enabled
      ? c.payment_mode === "test" ? "Test checkout · no real charges. Test credits are for this test environment only." : "Secure billing with Stripe. Manage your subscription and payment method here."
      : "Paid plans are coming soon. Your free monthly credits are available now.";
    const subscription = c.subscription || {status: "none", active: false};
    const renewal = subscription.current_period_end ? date(subscription.current_period_end) : "";
    $("billing-subscription-status").textContent = subscription.active
      ? subscription.cancel_at_period_end ? `Subscription ends ${renewal}. Unused credits stay in your account.` : `Subscription active${renewal ? ` · renews ${renewal}` : ""}.`
      : subscription.status === "past_due" || subscription.status === "unpaid" ? "Payment needs attention. Update your payment method to restore your subscription and top-ups."
      : subscription.status === "incomplete" ? "Subscription payment is pending. Complete payment before buying top-ups."
      : "You’re on Free. Keep 1,000 free credits every month; no subscription required.";
    $("billing-topup-status").textContent = c.can_top_up
      ? "Add credits whenever you need them. This is a one-time payment."
      : "Top-ups unlock with an active paid subscription.";
    $("manage-subscription").hidden = !c.portal_available;
    if (c.subscription_plan) {
      const plan = c.subscription_plan;
      $("billing-plan-price").textContent = new Intl.NumberFormat("en-US", {style: "currency", currency: plan.currency}).format(plan.amount_cents / 100);
      $("billing-plan-credits").textContent = Number(plan.credits).toLocaleString();
      $("subscribe-credits").hidden = !c.payments_enabled || !["none", "canceled", "incomplete_expired"].includes(subscription.status);
    }
    if (c.monthly) {
      $("monthly-credits").hidden = false;
      $("credit-month").textContent = `This month · ${c.monthly.month} (UTC)`;
      for (const name of ["free", "earned", "purchased", "spent"]) $("month-" + name).textContent = Number(c.monthly[name] || 0).toLocaleString();
    }
    const pack = c.credit_pack;
    if (pack) {
      const price = new Intl.NumberFormat("en-US", {style: "currency", currency: pack.currency}).format(pack.amount_cents / 100);
      $("billing-pack-price").textContent = price;
      $("billing-pack-credits").textContent = Number(pack.credits).toLocaleString();
      $("buy-credits").textContent = `Top up ${pack.credits.toLocaleString()} credits · ${price}`;
      $("buy-credits").hidden = !c.payments_enabled || !c.can_top_up;
      $("payment-info").hidden = false;
      $("payment-info").textContent = "Request costs: Search 1 credit · Answer 2 credits · Research 3 credits. Existing rate caps apply.";
    }
  }
}
let checkoutKey, subscriptionCheckoutKey;
async function startCheckout(kind) {
  const button = $(kind === "subscription" ? "subscribe-credits" : "buy-credits");
  if (button.disabled) return;
  button.disabled = true;
  const key = kind === "subscription" ? subscriptionCheckoutKey ||= crypto.randomUUID() : checkoutKey ||= crypto.randomUUID();
  try {
    const checkout = await api("payments/checkout", "POST", {kind, idempotency_key: key});
    const destination = new URL(checkout.url);
    if (destination.protocol !== "https:" || destination.host !== "checkout.stripe.com" || destination.username || destination.password)
      throw new Error("Invalid checkout destination.");
    location.assign(destination.href);
  } catch (e) {
    if (e.status === 409) {
      if (kind === "subscription") subscriptionCheckoutKey = undefined;
      else checkoutKey = undefined;
    }
    notify(e.message, true);
    button.disabled = false;
  }
}
$("buy-credits").onclick = () => startCheckout("topup");
$("subscribe-credits").onclick = () => startCheckout("subscription");
$("manage-subscription").onclick = async () => {
  const button = $("manage-subscription");
  if (button.disabled) return;
  button.disabled = true;
  try {
    const portal = await api("payments/portal", "POST", {});
    const destination = new URL(portal.url);
    if (destination.protocol !== "https:" || destination.host !== "billing.stripe.com" || destination.username || destination.password)
      throw new Error("Invalid billing portal destination.");
    location.assign(destination.href);
  } catch (e) { notify(e.message, true); button.disabled = false; }
};
async function showPaymentReturn() {
  const result = new URLSearchParams(location.search).get("payment");
  if (!result) return;
  history.replaceState(null, "", location.pathname);
  if (user) await view("billing");
  if (result === "cancelled") { notify("Checkout cancelled. No credits were added."); return; }
  if (result !== "success") return;
  notify("Checkout returned. Credits appear after Stripe confirms payment; this can take a moment.");
  // Display only: the browser cannot grant credits or confirm a charge.
  for (let i = 0; i < 4; i++) {
    await new Promise(resolve => setTimeout(resolve, 2000));
    if (!user) return;
    await refreshCredits();
  }
}
async function enter() {
  $("shared-nav").hidden = !sharedAuth || !user;
  await refreshLimits();
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
  if (name === "contribute" && !user) { showScreen("auth"); notify("Sign in to contribute webpages and earn credits."); return; }
  if (name === "billing" && !user) { showScreen("auth"); notify("Sign in to manage your credits."); return; }
  if (name === "shared" && (!sharedAuth || !user)) return;
  for (const value of ["search", "saved", "contribute", "shared", "connect", "billing"])
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
    if (name === "billing") await refreshCredits();
  } catch (e) {
    notify(e.message, true);
  }
}
document
  .querySelectorAll("[data-view]")
  .forEach((button) => (button.onclick = () => view(button.dataset.view)));
$("edit-interests").onclick = () => (user ? onboarding() : showScreen("auth"));
$("guest-signup").onclick = () => showScreen("auth");
$("entry-connect").onclick = async () => { try { await enter(); await view("connect"); } catch (e) { notify(e.message, true); } };
$("continue-guest").onclick = () => {
  if (authBusy) return;
  return enter().catch((e) => notify(e.message, true));
};
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
    : "One request available across Search, Research, and Answer. Sign in to contribute.";
}
setInterval(renderGuestAllowance, 1000);
$("logout").onclick = async () => {
  if (!user) {
    showScreen("auth");
    return;
  }
  if (authBusy) return;
  authBusy = true;
  try {
    // Invalidate outstanding responses before waiting for server-side revocation.
    resetAccount(user);
    await api("logout", "POST", {});
    resetAccount();
    showScreen("auth");
  } catch (e) {
    notify(e.message, true);
  } finally {
    authBusy = false;
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
  searchRequest?.abort();
  searchRequest = new AbortController();
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
    const data = await api(mode + "?q=" + encodeURIComponent(q), "GET", undefined, searchRequest);
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
  if (!user) { showScreen("auth"); notify("Sign in to contribute webpages and earn credits."); return; }
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
        "Sign in to contribute public webpages and track your contributions.",
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
let requestPolicy;
async function refreshLimits() {
  requestPolicy = await api("limits");
  const duration = (seconds) => seconds % 60 === 0 ? `${seconds / 60} min` : `${seconds} sec`;
  const describe = (limits) => Object.entries(limits).map(([mode, limit]) =>
    `${modeLabels[mode]} ${limit.requests}/${duration(limit.window_seconds)}`).join(" · ");
  const guestCooldowns = ["search", "answer", "research"].map((mode) =>
    `${modeLabels[mode]} ${duration(requestPolicy.guest[mode].window_seconds)}`).join(" · ");
  const guestPolicy = `Shared guest cooldown: ${guestCooldowns}. A request pauses all three modes.`;
  $("guest-policy").textContent = guestPolicy;
  $("request-limits").textContent = user
    ? `Search 1 credit · Answer 2 · Research 3. ${describe(requestPolicy.member)}.`
    : guestPolicy;
}
let startupPending = false;
async function initialize() {
  if (startupPending) return;
  startupPending = true;
  showScreen("boot");
  $("boot-message").textContent = "Loading your workspace…";
  $("boot-spinner").hidden = false;
  $("boot-retry").hidden = true;
  const generation = accountGeneration;
  try {
    const authConfig = await api("auth/config");
    sharedAuth = authConfig.shared === true;
    supportsPassword = sharedAuth && authConfig.supports_password === true;
    authConfigured = true;
    $("auth-submit").disabled = false;
    if (sharedAuth) {
      renderSharedAuth();
      $("interests-explanation").textContent = "Save interests to follow these topics across Cosift and your connected agents. Existing agent topics stay followed; remove them in Followed topics.";
    }
    try { user = await api("me"); }
    catch (e) { if (e.status !== 401) throw e; user = null; }
    await refreshLimits();
    if (location.pathname === "/login" && signingUp) $("auth-toggle").click();
    if (user) await enter();
    else showScreen("auth");
    showPaymentReturn().catch(e => notify(e.message, true));
  } catch (e) {
    if (generation !== accountGeneration || e.name === "AbortError") return;
    if ($("boot").hidden) { notify(e.message, true); return; }
    $("boot-message").textContent = "We couldn’t load your workspace. Your session has not been cleared. Please try again.";
    $("boot-spinner").hidden = true;
    $("boot-retry").hidden = false;
  } finally {
    startupPending = false;
  }
}
$("boot-retry").onclick = initialize;
initialize();

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

async function refreshSharedTopics() {
  const data = await api("shared", "POST", {tool:"cosift_topics", action:"list"});
  $("shared-list").replaceChildren();
  const items = data.topics || [];
  if (!items.length) $("shared-list").append(el("p", "No followed topics yet.", "muted"));
  for (const item of items) {
    const topic = item.topic_text || item.topic || "";
    const card = el("article", undefined, "panel");
    card.append(el("h3", topic));
    if (item.requested) card.append(el("p", "Article requested. Request history is retained when unfollowing.", "fine"));
    const remove = el("button", "Unfollow", "text-button");
    remove.onclick = async () => {
      remove.disabled = true;
      try { await api("shared", "POST", {tool:"cosift_topics", action:"remove", topics:[topic]}); await refreshSharedTopics(); }
      catch (e) { notify(e.message,true); remove.disabled=false; }
    };
    card.append(remove); $("shared-list").append(card);
  }
}
$("shared-refresh").onclick = () => refreshSharedTopics().catch(e => notify(e.message,true));
$("shared-nav").onclick = () => { view("shared"); refreshSharedTopics().catch(e => notify(e.message,true)); };
$("shared-form").onsubmit = event => {
  event.preventDefault();
  busy(event.target,async () => {
    const action = $("shared-action").value, topic = $("shared-topic").value.trim();
    const payload = action === "add" ? {tool:"cosift_topics",action:"add",topics:[topic]} : {tool:action,topic};
    const data = await api("shared","POST",payload);
    $("shared-result").replaceChildren();
    if (action === "add") $("shared-result").append(el("p","Topic followed."));
    else if (action === "cosift_request") $("shared-result").append(el("p",data.detail || "Article request recorded."));
    else {
      $("shared-result").append(el("p",data.coverage === "covered" ? "Cosift has an article on this topic." : "No complete article is available yet. You can still search the webpage index."));
      const article = data.kind === "article" ? data : data.related_article;
      if (article) {
        $("shared-result").append(el("p",article.text || ""));
        for (const citation of article.citations || []) {
          const url = typeof citation === "string" ? citation : citation.url;
          $("shared-result").append(link(url, typeof citation === "string" ? citation : citation.title || url));
        }
      }
    }
    await refreshSharedTopics();
  });
};

$("auth-restart").onclick = () => {
  if (authBusy) return;
  resetAccount();
  authChallenge = null;
  const form = $("auth-form");
  form.elements.code.value = "";
  form.elements.code.required = false;
  form.elements.email.readOnly = false;
  $("code-field").hidden = true;
  $("auth-restart").hidden = true;
  $("auth-submit").textContent = "Email me a code →";
  form.elements.password.value = "";
  if (sharedAuth) renderSharedAuth();
};

// The application sends only sanitized pageviews, with no search/account payload.
// Disable Enhanced Measurement in the GA stream for a pageview-only setup.
async function loadAnalytics() {
  if (typeof window === "undefined") return;
  try {
    const response = await fetch("/api/analytics", {credentials: "same-origin"});
    if (!response.ok) return;
    const config = await response.json();
    if (!/^G-[A-Z0-9]+$/.test(config.measurement_id || "")) return;
    window.dataLayer = window.dataLayer || [];
    const gtag = function () { window.dataLayer.push(arguments); };
    window.gtag = gtag;
    gtag("js", new Date());
    gtag("config", config.measurement_id, {send_page_view: false, allow_google_signals: false, allow_ad_personalization_signals: false, page_location: location.origin + location.pathname, page_referrer: ""});
    gtag("event", "page_view", {page_location: location.origin + location.pathname, page_title: "Cosift", page_referrer: ""});
    const script = document.createElement("script");
    script.async = true;
    script.src = "https://www.googletagmanager.com/gtag/js?id=" + config.measurement_id;
    document.head.append(script);
  } catch (_) { /* Analytics failure must not block the app. */ }
}
loadAnalytics();
