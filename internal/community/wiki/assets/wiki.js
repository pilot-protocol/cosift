// Optional analytics for the article pages: one sanitized pageview, no signals.
(function () {
  var tag = document.querySelector("script[data-ga]");
  var id = tag && tag.getAttribute("data-ga");
  if (!/^G-[A-Z0-9]+$/.test(id || "")) return;
  window.dataLayer = window.dataLayer || [];
  var gtag = function () { window.dataLayer.push(arguments); };
  window.gtag = gtag;
  var page = location.origin + location.pathname;
  gtag("js", new Date());
  gtag("config", id, {send_page_view: false, allow_google_signals: false, allow_ad_personalization_signals: false, page_location: page, page_referrer: ""});
  gtag("event", "page_view", {page_location: page, page_title: document.title, page_referrer: ""});
  var script = document.createElement("script");
  script.async = true;
  script.src = "https://www.googletagmanager.com/gtag/js?id=" + id;
  document.head.appendChild(script);
})();
