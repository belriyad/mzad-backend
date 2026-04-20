self.addEventListener("push", (event) => {
  let data = {};
  try {
    data = event.data ? event.data.json() : {};
  } catch (_e) {
    data = { title: "New Car Match", body: "You have a new notification." };
  }

  const title = data.title || "New Car Match";
  const options = {
    body: data.body || "You have a new listing match.",
    icon: data.icon || data.image || "/favicon.ico",
    data: {
      url: data.url || "",
    },
  };

  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const targetUrl = event.notification.data?.url || "/";
  event.waitUntil(clients.openWindow(targetUrl));
});
