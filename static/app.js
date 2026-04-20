async function getJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function postJson(url, payload) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}

function n(v) {
  return v === null || v === undefined || v === "" ? "—" : Number(v).toLocaleString("en-US");
}

function esc(v) {
  return String(v ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function yearText(v) {
  if (v === null || v === undefined || v === "") return "—";
  return String(v);
}

function proxiedImageUrl(rawUrl) {
  const value = String(rawUrl || "").trim();
  if (!value) return "";
  try {
    const u = new URL(value);
    const host = (u.hostname || "").toLowerCase();
    const allowed = ["files.qatarliving.com", "images.qatarliving.com", "content.mzadqatar.com"];
    if (allowed.some((h) => host === h || host.endsWith(`.${h}`))) {
      return `/api/img-proxy?url=${encodeURIComponent(value)}`;
    }
    return value;
  } catch {
    return value;
  }
}

function dtText(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString();
}

function durationText(seconds) {
  if (seconds === null || seconds === undefined || Number(seconds) < 0) return "—";
  const s = Number(seconds);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h}h ${m}m ${sec}s`;
  if (m > 0) return `${m}m ${sec}s`;
  return `${sec}s`;
}

function setupDualRange(prefix, min, max, step = 1, formatter = (x) => `${x}`) {
  const minSlider = document.getElementById(`${prefix}-min-slider`);
  const maxSlider = document.getElementById(`${prefix}-max-slider`);
  const minLabel = document.getElementById(`${prefix}-min-val`);
  const maxLabel = document.getElementById(`${prefix}-max-val`);

  minSlider.min = String(min);
  minSlider.max = String(max);
  minSlider.step = String(step);
  maxSlider.min = String(min);
  maxSlider.max = String(max);
  maxSlider.step = String(step);

  minSlider.value = String(min);
  maxSlider.value = String(max);

  const sync = () => {
    if (Number(minSlider.value) > Number(maxSlider.value)) {
      if (document.activeElement === minSlider) maxSlider.value = minSlider.value;
      else minSlider.value = maxSlider.value;
    }
    minLabel.textContent = formatter(Number(minSlider.value));
    maxLabel.textContent = formatter(Number(maxSlider.value));
  };
  minSlider.addEventListener("input", sync);
  maxSlider.addEventListener("input", sync);
  sync();

  return {
    getMin: () => Number(minSlider.value),
    getMax: () => Number(maxSlider.value),
  };
}

let yearRange;
let priceRange;
let kmRange;
let discountRange;
let peerRange;
let notificationsTimer = null;
let cachedOptions = { makes: [], classes: [], models: [], cities: [] };
let browserNotifyEnabled = localStorage.getItem("browser_notify_enabled") === "1";
let webPushSubscribed = false;
let webPushConfigured = false;

function isNotificationSecureContext() {
  const h = window.location.hostname;
  return window.isSecureContext || h === "localhost" || h === "127.0.0.1" || h === "::1";
}

function browserNotifySupported() {
  return "Notification" in window && isNotificationSecureContext();
}

function pushSupportedInBrowser() {
  return (
    isNotificationSecureContext() &&
    "serviceWorker" in navigator &&
    "PushManager" in window &&
    "Notification" in window
  );
}

function urlBase64ToUint8Array(base64String) {
  const padding = "=".repeat((4 - (base64String.length % 4)) % 4);
  const base64 = (base64String + padding).replace(/-/g, "+").replace(/_/g, "/");
  const rawData = atob(base64);
  const out = new Uint8Array(rawData.length);
  for (let i = 0; i < rawData.length; i += 1) out[i] = rawData.charCodeAt(i);
  return out;
}

async function ensureServiceWorker() {
  return navigator.serviceWorker.register("/sw.js");
}

function updateBrowserNotifyStatus() {
  const el = document.getElementById("browser-notify-status");
  const enableBtn = document.getElementById("enable-browser-notify");
  const testBtn = document.getElementById("test-browser-notify");
  if (!el) return;
  if (!isNotificationSecureContext()) {
    el.textContent = "Requires HTTPS or localhost";
    if (enableBtn) enableBtn.disabled = true;
    if (testBtn) testBtn.disabled = true;
    return;
  }
  if (!pushSupportedInBrowser()) {
    el.textContent = "Push not supported by this browser";
    if (enableBtn) enableBtn.disabled = true;
    if (testBtn) testBtn.disabled = true;
    return;
  }
  if (enableBtn) enableBtn.disabled = false;
  if (testBtn) testBtn.disabled = !webPushSubscribed;
  const perm = Notification.permission;
  const configured = webPushConfigured ? "server-ready" : "server-not-configured";
  const subscribed = webPushSubscribed ? "subscribed" : "not-subscribed";
  const tabAlerts = browserNotifyEnabled ? "tab-alerts:on" : "tab-alerts:off";
  el.textContent = `Permission: ${perm} | ${configured} | ${subscribed} | ${tabAlerts}`;
}

async function enableBrowserNotifications() {
  if (!isNotificationSecureContext()) {
    throw new Error("Browser notifications need HTTPS (or open this UI on localhost).");
  }
  if (!pushSupportedInBrowser()) throw new Error("This browser does not support Web Push.");
  const user_key = getUserKey();
  if (!user_key) throw new Error("Enter a user key first.");

  const keyInfo = await getJson("/api/push/public-key");
  webPushConfigured = Boolean(keyInfo.configured);
  if (!webPushConfigured || !keyInfo.public_key) {
    updateBrowserNotifyStatus();
    throw new Error("Server push key is not configured.");
  }

  const permission = await Notification.requestPermission();
  if (permission !== "granted") {
    browserNotifyEnabled = false;
    webPushSubscribed = false;
    localStorage.setItem("browser_notify_enabled", "0");
    updateBrowserNotifyStatus();
    throw new Error("Notification permission was not granted.");
  }

  const reg = await ensureServiceWorker();
  let subscription = await reg.pushManager.getSubscription();
  if (!subscription) {
    subscription = await reg.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8Array(keyInfo.public_key),
    });
  }
  await postJson("/api/push/subscribe", {
    user_key,
    subscription: subscription.toJSON(),
  });

  browserNotifyEnabled = true;
  webPushSubscribed = true;
  localStorage.setItem("browser_notify_enabled", "1");
  updateBrowserNotifyStatus();
}

function notifyBrowser(title, body, url, imageUrl) {
  if (!browserNotifySupported()) return;
  if (!browserNotifyEnabled) return;
  if (Notification.permission !== "granted") return;
  const notification = new Notification(title, {
    body,
    icon: imageUrl || undefined,
  });
  notification.onclick = () => {
    if (url) window.open(url, "_blank", "noopener,noreferrer");
  };
}

async function refreshPushStatus() {
  const user_key = getUserKey();
  if (!user_key) {
    webPushConfigured = false;
    webPushSubscribed = false;
    updateBrowserNotifyStatus();
    return;
  }
  const data = await getJson(`/api/push/status?user_key=${encodeURIComponent(user_key)}`);
  webPushConfigured = Boolean(data.configured);
  webPushSubscribed = Number(data.active_subscriptions || 0) > 0;
  updateBrowserNotifyStatus();
}

async function loadChannels() {
  const user_key = getUserKey();
  const statusEl = document.getElementById("wa-status");
  if (!user_key) {
    document.getElementById("wa-number").value = "";
    document.getElementById("wa-enabled").checked = false;
    statusEl.textContent = "Enter user key";
    return;
  }
  const data = await getJson(`/api/channels?user_key=${encodeURIComponent(user_key)}`);
  const row = data.row || {};
  document.getElementById("wa-number").value = row.whatsapp_number ? String(row.whatsapp_number).replace(/^whatsapp:/, "") : "";
  document.getElementById("wa-enabled").checked = Number(row.whatsapp_enabled || 0) === 1;
  const serverCfg = data.whatsapp_configured ? "Twilio ready" : "Twilio not configured";
  const userCfg = row.whatsapp_number ? "number saved" : "no number";
  statusEl.textContent = `${serverCfg} | ${userCfg}`;
}

async function saveChannels() {
  const user_key = getUserKey();
  if (!user_key) throw new Error("Enter a user key first.");
  const whatsapp_number = document.getElementById("wa-number").value.trim();
  const whatsapp_enabled = document.getElementById("wa-enabled").checked;
  await postJson("/api/channels", { user_key, whatsapp_number, whatsapp_enabled });
  await loadChannels();
}

async function testWhatsappChannel() {
  const user_key = getUserKey();
  if (!user_key) throw new Error("Enter a user key first.");
  const resp = await postJson("/api/channels/test-whatsapp", { user_key });
  if (!resp.ok) {
    throw new Error(resp.reason || resp.message || "WhatsApp test failed.");
  }
  return resp;
}

function polar(cx, cy, r, deg) {
  const rad = (deg * Math.PI) / 180;
  return { x: cx + r * Math.cos(rad), y: cy + r * Math.sin(rad) };
}

function moveLine(id, p1, p2) {
  const el = document.getElementById(id);
  if (!el) return;
  el.setAttribute("x1", String(p1.x));
  el.setAttribute("y1", String(p1.y));
  el.setAttribute("x2", String(p2.x));
  el.setAttribute("y2", String(p2.y));
}

function drawValueDial(low, fair, high, markerPrice) {
  const arc = document.getElementById("dial-arc");
  const cx = 140;
  const cy = 140;
  const r = 110;
  const pStart = polar(cx, cy, r, 180);
  const pEnd = polar(cx, cy, r, 0);
  if (arc) arc.setAttribute("d", `M ${pStart.x} ${pStart.y} A ${r} ${r} 0 0 1 ${pEnd.x} ${pEnd.y}`);

  const span = Math.max(1, high - low);
  const toDeg = (v) => {
    const t = Math.max(0, Math.min(1, (v - low) / span));
    return 180 - t * 180;
  };

  const lowDeg = toDeg(low);
  const fairDeg = toDeg(fair);
  const highDeg = toDeg(high);
  const userDeg = toDeg(markerPrice);

  moveLine("dial-low-mark", polar(cx, cy, r - 4, lowDeg), polar(cx, cy, r - 26, lowDeg));
  moveLine("dial-fair-mark", polar(cx, cy, r - 2, fairDeg), polar(cx, cy, r - 34, fairDeg));
  moveLine("dial-high-mark", polar(cx, cy, r - 4, highDeg), polar(cx, cy, r - 26, highDeg));
  moveLine("dial-user-mark", polar(cx, cy, r - 1, userDeg), polar(cx, cy, r - 44, userDeg));
}

function fillValueInputsFromFilters() {
  document.getElementById("val-make").value = document.getElementById("make").value || "";
  document.getElementById("val-class").value = document.getElementById("class_name").value || "";
  document.getElementById("val-model").value = document.getElementById("model").value || "";
  document.getElementById("val-year").value = String(yearRange.getMax());
  document.getElementById("val-km").value = String(kmRange.getMin());
}

async function checkValueEstimate() {
  const make = document.getElementById("val-make").value.trim();
  const class_name = document.getElementById("val-class").value.trim();
  const model = document.getElementById("val-model").value.trim();
  const year = document.getElementById("val-year").value.trim();
  const km = document.getElementById("val-km").value.trim();
  if (!make || !class_name || !year || !km) {
    throw new Error("Select make, class, year, and mileage.");
  }

  const q = new URLSearchParams({
    make,
    class_name,
    year,
    km,
  });
  if (model) q.set("model", model);
  const data = await getJson(`/api/value-estimate?${q.toString()}`);
  if (!data.ok) {
    document.getElementById("value-summary").textContent =
      data.reason === "not_enough_peers" ? `Not enough peers (${data.peer_count || 0})` : "No comparables found.";
    return;
  }

  const low = Number(data.low_price_qar || 0);
  const fair = Number(data.fair_price_qar || 0);
  const high = Number(data.high_price_qar || 0);
  drawValueDial(low, fair, high, fair);
  document.getElementById("value-summary").textContent =
    `Low ${n(low)} | Fair ${n(fair)} | High ${n(high)} QAR | Peers ${data.peer_count}`;
}

async function loadSummary() {
  const data = await getJson("/api/summary");
  const top = (data.topMakes || []).map((x) => `${x.name} (${x.count})`).join(" • ");
  const phone = data.phoneStats || {};
  const sourceBits = (data.sourceStats || [])
    .map((s) => `${s.source}: ${n(s.listings)} listings, ${n(s.uniquePhoneNumbers)} phones`)
    .join(" | ");
  document.getElementById("summary").textContent = `Total listings: ${n(data.totalListings)} | Listings with phone: ${n(phone.listingsWithPhone || 0)} | Unique phone numbers: ${n(phone.uniquePhoneNumbers || 0)}${top ? ` | Top makes: ${top}` : ""}`;
  document.getElementById("summary-extra").textContent = `Phone coverage: ${Number(phone.coveragePct || 0).toFixed(2)}%`;
  if (sourceBits) {
    document.getElementById("summary-extra").textContent += ` | ${sourceBits}`;
  }

  const runStats = data.collectionRunStats || {};
  const runMeta = `Runs: ${n(runStats.totalRuns || 0)} | Total new rows: ${n(runStats.totalNewRows || 0)} | Total pages scanned: ${n(runStats.totalPagesScanned || 0)} | Avg new rows/run: ${Number(runStats.avgNewRowsPerRun || 0).toFixed(2)} | Last finish: ${dtText(runStats.lastFinishedAt)}`;
  document.getElementById("summary-runs-meta").textContent = runMeta;

  const runs = data.collectionRuns || [];
  const tbody = document.getElementById("summary-runs-body");
  if (!runs.length) {
    tbody.innerHTML = `<tr><td colspan="7">No run data yet.</td></tr>`;
    return;
  }
  tbody.innerHTML = runs
    .map(
      (r) => `
      <tr>
        <td>${n(r.id)}</td>
        <td>${esc(dtText(r.startedAt))}</td>
        <td>${esc(dtText(r.finishedAt))}</td>
        <td>${esc(durationText(r.durationSeconds))}</td>
        <td>${n(r.pagesScanned)}</td>
        <td>${n(r.newRows)}</td>
        <td>${esc(r.notes || "")}</td>
      </tr>`
    )
    .join("");
}

async function loadRanges() {
  const r = await getJson("/api/ranges");
  yearRange = setupDualRange("year", r.year.min, r.year.max, 1, (v) => `${v}`);
  priceRange = setupDualRange("price", r.price.min, r.price.max, 1000, (v) => `${n(v)} QAR`);
  kmRange = setupDualRange("km", r.km.min, r.km.max, 1000, (v) => `${n(v)} KM`);
  discountRange = setupDualRange("discount", r.discount_pct.min, r.discount_pct.max, 1, (v) => `${v}%`);
  peerRange = setupDualRange("peer", r.peer_count.min, r.peer_count.max, 1, (v) => `${v}`);
  populateValuatorSelectors();
}

async function loadListings() {
  const source = document.getElementById("source").value || "all";
  const search = document.getElementById("search").value.trim();
  const make = document.getElementById("make").value;
  const class_name = document.getElementById("class_name").value;
  const model = document.getElementById("model").value;
  const city = document.getElementById("city").value;
  const sort = document.getElementById("sort").value;
  const dealsOnly = document.getElementById("deals-only").value;
  const limit = document.getElementById("limit").value || "100";

  const q = new URLSearchParams({ sort, limit });
  if (source && source !== "all") q.set("source", source);
  if (search) q.set("search", search);
  if (make) q.set("make", make);
  if (class_name) q.set("class_name", class_name);
  if (model) q.set("model", model);
  if (city) q.set("city", city);
  q.set("deals_only", dealsOnly);
  q.set("min_discount_pct", String(discountRange.getMin()));
  q.set("max_discount_pct", String(discountRange.getMax()));
  q.set("min_peer_count", String(peerRange.getMin()));
  q.set("max_peer_count", String(peerRange.getMax()));
  q.set("min_year", String(yearRange.getMin()));
  q.set("max_year", String(yearRange.getMax()));
  q.set("min_price", String(priceRange.getMin()));
  q.set("max_price", String(priceRange.getMax()));
  q.set("min_km", String(kmRange.getMin()));
  q.set("max_km", String(kmRange.getMax()));

  const data = await getJson(`/api/listings?${q.toString()}`);
  const rows = data.rows || [];
  document.getElementById("meta").textContent = `${rows.length} row(s)`;
  document.getElementById("rows").innerHTML = rows
    .map(
      (r) => `
      <tr>
        <td>${r.main_image_url ? `<img src="${esc(proxiedImageUrl(r.main_image_url))}" alt="${esc(r.title)}" loading="lazy" onerror="this.onerror=null;this.src='/placeholder-car.svg';" style="width:72px;height:54px;object-fit:cover;border-radius:6px;" />` : "—"}</td>
        <td>${esc(r.product_id)}</td>
        <td>${esc(r.title)}</td>
        <td>${n(r.price_qar)}</td>
        <td>${esc(r.make)}</td>
        <td>${esc(r.class_name)}</td>
        <td>${yearText(r.manufacture_year)}</td>
        <td>${n(r.km)}</td>
        <td>${esc(r.warranty_status || "") || "—"}</td>
        <td>${n(r.cylinder_count)}</td>
        <td>${esc(r.listing_date || r.advertise_time_formatted || "") || "—"}</td>
        <td>${n(r.expected_price_qar)}</td>
        <td>${r.discount_pct === null || r.discount_pct === undefined ? "—" : `${Number(r.discount_pct).toFixed(2)}%`}</td>
        <td>${r.deal_score === null || r.deal_score === undefined ? "—" : Number(r.deal_score).toFixed(2)}</td>
        <td>${n(r.peer_count)}</td>
        <td>${esc(r.city)}</td>
        <td><a href="${esc(r.url)}" target="_blank" rel="noreferrer">Open</a></td>
      </tr>
    `
    )
    .join("");

  cachedOptions = {
    makes: data.makes || [],
    classes: data.classes || [],
    models: data.models || [],
    cities: data.cities || [],
  };
  setSelectOptions("make", "All makes", cachedOptions.makes);
  setSelectOptions("class_name", "All classes", cachedOptions.classes);
  setSelectOptions("model", "All models", cachedOptions.models);
  setSelectOptions("city", "All cities", cachedOptions.cities);
  populateValuatorSelectors();
}

function setSelectOptions(id, firstLabel, values) {
  const sel = document.getElementById(id);
  const current = sel.value;
  const html = [`<option value="">${firstLabel}</option>`]
    .concat(values.map((v) => `<option value="${esc(v)}">${esc(v)}</option>`))
    .join("");
  sel.innerHTML = html;
  sel.value = values.includes(current) ? current : "";
}

function setNumberSelectOptions(id, firstLabel, values, suffix = "") {
  const sel = document.getElementById(id);
  const current = sel.value;
  const html = [`<option value="">${firstLabel}</option>`]
    .concat(
      values.map((v) => {
        const label = suffix ? `${n(v)} ${suffix}` : `${v}`;
        return `<option value="${v}">${label}</option>`;
      })
    )
    .join("");
  sel.innerHTML = html;
  sel.value = values.map(String).includes(current) ? current : "";
}

function populateValuatorSelectors() {
  setSelectOptions("val-make", "Select make", cachedOptions.makes || []);
  setSelectOptions("val-class", "Select class", cachedOptions.classes || []);
  setSelectOptions("val-model", "Any model", cachedOptions.models || []);

  if (yearRange) {
    const years = [];
    for (let y = yearRange.getMax(); y >= yearRange.getMin(); y -= 1) years.push(y);
    setNumberSelectOptions("val-year", "Select year", years);
  }

  if (kmRange) {
    const vals = [];
    const minKm = kmRange.getMin();
    const maxKm = kmRange.getMax();
    const step = maxKm - minKm > 250000 ? 10000 : 5000;
    for (let v = minKm; v <= maxKm; v += step) vals.push(v);
    if (vals[vals.length - 1] !== maxKm) vals.push(maxKm);
    setNumberSelectOptions("val-km", "Select mileage", vals, "KM");
  }
}

function getUserKey() {
  return document.getElementById("notify-user-key").value.trim();
}

function renderAlerts(rows) {
  const el = document.getElementById("alerts-list");
  if (!rows.length) {
    el.innerHTML = '<div class="panel-item">No saved alerts.</div>';
    return;
  }
  el.innerHTML = rows
    .map((r) => {
      const bits = [];
      if (r.make) bits.push(r.make);
      if (r.class_name) bits.push(r.class_name);
      if (r.model) bits.push(r.model);
      if (r.city) bits.push(r.city);
      if (r.min_price_qar || r.max_price_qar) bits.push(`QAR ${n(r.min_price_qar)}-${n(r.max_price_qar)}`);
      if (r.min_year || r.max_year) bits.push(`Year ${r.min_year || "Any"}-${r.max_year || "Any"}`);
      return `<div class="panel-item"><strong>#${r.id}</strong> ${esc(bits.join(" | ") || "Any car")} ${r.active ? "" : "(paused)"}</div>`;
    })
    .join("");
}

function renderNotifications(rows) {
  const el = document.getElementById("notifications-list");
  if (!rows.length) {
    el.innerHTML = '<div class="panel-item">No notifications.</div>';
    return;
  }
  el.innerHTML = rows
    .map(
      (r) =>
        `<div class="panel-item">${r.is_read ? "" : "<strong>[NEW]</strong> "}${esc(r.message || r.title || "")}<br/><small>${esc(r.created_at || "")}</small></div>`
    )
    .join("");
}

async function loadAlerts() {
  const user_key = getUserKey();
  if (!user_key) {
    renderAlerts([]);
    return;
  }
  const data = await getJson(`/api/alerts?user_key=${encodeURIComponent(user_key)}`);
  renderAlerts(data.rows || []);
}

async function loadNotifications() {
  const user_key = getUserKey();
  if (!user_key) {
    renderNotifications([]);
    return;
  }
  const unreadOnly = document.getElementById("unread-only").checked ? "1" : "0";
  const data = await getJson(
    `/api/notifications?user_key=${encodeURIComponent(user_key)}&unread_only=${unreadOnly}&limit=100`
  );
  const rows = data.rows || [];
  renderNotifications(rows);

  const storageKey = `notify_last_seen_${user_key}`;
  const maxId = rows.reduce((acc, r) => Math.max(acc, Number(r.id) || 0), 0);
  let lastSeen = Number(localStorage.getItem(storageKey) || "0");
  if (lastSeen === 0 && maxId > 0) {
    localStorage.setItem(storageKey, String(maxId));
    lastSeen = maxId;
  }
  const newRows = rows.filter((r) => !r.is_read && (Number(r.id) || 0) > lastSeen);
  if (newRows.length) {
    newRows
      .slice()
      .reverse()
      .slice(0, 3)
      .forEach((r) => {
        notifyBrowser("New Car Match", r.message || r.title || "New listing", r.url, r.main_image_url);
      });
    localStorage.setItem(storageKey, String(maxId));
  }
}

async function saveAlertFromFilters() {
  const user_key = getUserKey();
  if (!user_key) throw new Error("Enter a user key first.");
  const payload = {
    user_key,
    make: document.getElementById("make").value || null,
    class_name: document.getElementById("class_name").value || null,
    model: document.getElementById("model").value || null,
    city: document.getElementById("city").value || null,
    search_text: document.getElementById("search").value.trim() || null,
    min_year: yearRange.getMin(),
    max_year: yearRange.getMax(),
    min_price_qar: priceRange.getMin(),
    max_price_qar: priceRange.getMax(),
    min_km: kmRange.getMin(),
    max_km: kmRange.getMax(),
    deals_only: document.getElementById("deals-only").value === "1",
    min_discount_pct: discountRange.getMin(),
    max_discount_pct: discountRange.getMax(),
    min_peer_count: peerRange.getMin(),
    max_peer_count: peerRange.getMax(),
  };
  await postJson("/api/alerts", payload);
  await postJson("/api/collector/run", {});
  await loadAlerts();
  await loadNotifications();
}

async function markAllRead() {
  const user_key = getUserKey();
  if (!user_key) throw new Error("Enter a user key first.");
  await postJson("/api/notifications/mark-read", { user_key });
  await loadNotifications();
}

function startNotificationsPolling() {
  if (notificationsTimer) clearInterval(notificationsTimer);
  notificationsTimer = setInterval(() => {
    loadNotifications().catch(() => {});
  }, 7000);
}

async function run() {
  await loadSummary();
  await loadRanges();
  await loadListings();
  const remembered = localStorage.getItem("notify_user_key");
  if (remembered) document.getElementById("notify-user-key").value = remembered;
  await refreshPushStatus();
  await loadChannels();
  await loadAlerts();
  await loadNotifications();
  startNotificationsPolling();
}

document.getElementById("run").addEventListener("click", () => {
  loadListings().catch((e) => alert(e.message));
});
document.getElementById("save-alert").addEventListener("click", () => {
  saveAlertFromFilters().then(() => alert("Alert saved.")).catch((e) => alert(e.message));
});
document.getElementById("refresh-notify").addEventListener("click", () => {
  Promise.all([loadAlerts(), loadNotifications(), loadChannels()]).catch((e) => alert(e.message));
});
document.getElementById("mark-all-read").addEventListener("click", () => {
  markAllRead().catch((e) => alert(e.message));
});
document.getElementById("notify-user-key").addEventListener("change", () => {
  const key = getUserKey();
  localStorage.setItem("notify_user_key", key);
  Promise.all([loadAlerts(), loadNotifications(), refreshPushStatus(), loadChannels()]).catch(() => {});
});
document.getElementById("save-wa-channel").addEventListener("click", () => {
  saveChannels()
    .then(() => alert("WhatsApp channel saved."))
    .catch((e) => alert(e.message));
});
document.getElementById("test-wa-channel").addEventListener("click", () => {
  testWhatsappChannel()
    .then(() => alert("WhatsApp test message sent."))
    .catch((e) => alert(e.message));
});
document.getElementById("check-value").addEventListener("click", () => {
  checkValueEstimate().catch((e) => alert(e.message));
});
document.getElementById("fill-from-filters").addEventListener("click", () => {
  fillValueInputsFromFilters();
});
document.getElementById("enable-browser-notify").addEventListener("click", () => {
  enableBrowserNotifications()
    .then(() => alert("Web Push enabled for this user on this device."))
    .catch((e) => alert(e.message));
});
document.getElementById("test-browser-notify").addEventListener("click", () => {
  const user_key = getUserKey();
  if (!user_key) {
    alert("Enter a user key first.");
    return;
  }
  postJson("/api/push/test", { user_key })
    .then((resp) => {
      if (resp.ok) alert("Push test sent.");
      else alert(`Push test failed: ${resp.reason || "no active subscription"}`);
    })
    .catch((e) => alert(e.message));
});
document.getElementById("unread-only").addEventListener("change", () => {
  loadNotifications().catch(() => {});
});
document.getElementById("search").addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    loadListings().catch((err) => alert(err.message));
  }
});

run().catch((e) => {
  document.body.innerHTML = `<pre>Failed to load UI: ${e.message}</pre>`;
});
