import { consoleRequest } from './api.mjs';

export const NotificationDefaults = Object.freeze({
  game_purchase_correction_message: 'A purchase correction has been issued to you.',
  game_resource_grant_message: 'Resources have been added to your account.',
});
export async function loadNotificationDefaults(options = {}) {
  const response = await consoleRequest('/v2/console/setting', null, { ...options, method: 'GET' });
  const result = await response.json();
  const values = { ...NotificationDefaults };
  for (const setting of result.settings || []) {
    if (!Object.hasOwn(values, setting.name)) continue;
    try { const value = JSON.parse(setting.value); if (typeof value === 'string' && value.trim()) values[setting.name] = value; } catch {}
  }
  return values;
}
export function applyNotificationDefault(field, key, signal) {
  field.value = NotificationDefaults[key];
  let edited = false;
  field.addEventListener('input', () => { edited = true; }, { signal });
  loadNotificationDefaults({ fetchFn: (url, options) => fetch(url, { ...options, signal }) }).then(values => {
    if (!signal.aborted && field.isConnected && !edited) field.value = values[key];
  }).catch(() => {});
}
export function mountNotificationSettings(nativeCard) {
  const abort = new AbortController();
  const card = document.createElement('section');
  card.className = nativeCard.className + ' cardgame-notification-settings';
  card.innerHTML = '<div class="px-4 py-3 border-b flex items-center"><h2 class="text-lg font-medium">Game Notifications</h2></div><div class="cardgame-notification-panel" data-settings-body></div>';
  nativeCard.before(card);
  const forms = new Map();
  for (const [key, label] of [['game_purchase_correction_message', 'Default purchase correction message'], ['game_resource_grant_message', 'Default resource grant message']]) {
    const form = document.createElement('form');
    form.innerHTML = '<fieldset disabled><label></label><button type="submit" class="cardgame-primary">Save</button></fieldset><p data-status role="status" aria-live="polite">Loading…</p>';
    const labelElement = form.querySelector('label'); labelElement.append(document.createTextNode(label));
    const field = document.createElement('textarea'); field.required = true; field.maxLength = 8000; field.rows = 3; field.dir = 'auto'; field.name = key;
    labelElement.append(field); forms.set(key, { form, field }); card.querySelector('[data-settings-body]').append(form);
    form.addEventListener('submit', async event => {
      event.preventDefault(); const fields = form.querySelector('fieldset'); if (fields.disabled) return;
      const status = form.querySelector('[data-status]'); fields.disabled = true; status.dataset.error = 'false'; status.textContent = '';
      try {
        await consoleRequest(`/v2/console/setting/${key}`, { value: JSON.stringify(field.value) }, { fetchFn: (url, options) => fetch(url, { ...options, signal: abort.signal }) });
        if (!abort.signal.aborted) status.textContent = 'Saved.';
      } catch (error) { if (!abort.signal.aborted) { status.dataset.error = 'true'; status.textContent = error.message; } }
      finally { fields.disabled = false; }
    });
  }
  async function load() {
    try {
      const values = await loadNotificationDefaults({ fetchFn: (url, options) => fetch(url, { ...options, signal: abort.signal }) });
      if (abort.signal.aborted) return;
      for (const [key, { form, field }] of forms) { field.value = values[key]; form.querySelector('fieldset').disabled = false; form.querySelector('[data-status]').textContent = ''; }
    } catch (error) {
      if (abort.signal.aborted) return;
      for (const { form } of forms.values()) { const status = form.querySelector('[data-status]'); status.dataset.error = 'true'; status.textContent = error.message; }
      const retry = document.createElement('button'); retry.type = 'button'; retry.textContent = 'Retry loading settings'; retry.addEventListener('click', () => { retry.remove(); load(); }); card.querySelector('[data-settings-body]').append(retry);
    }
  }
  load();
  return { card, dispose() { abort.abort(); card.remove(); } };
}
