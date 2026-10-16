import { buildNotification, sendNotification } from './model.mjs';
import { mountPopupTabs } from './popup-tabs.mjs';

export function mountNotificationPopup(dialog, nativePanel, userId, { allUsers = false } = {}) {
  const abort = new AbortController();
  const panel = document.createElement('section');
  panel.className = 'cardgame-notification-panel';
  panel.dataset.notificationForm = '';
  panel.innerHTML = `<h2>Send a game notification</h2><p data-player></p><fieldset disabled>
    <label>Notification type<select name="type" aria-label="Notification type"><option value="2">Message</option><option value="3">Message with Link</option></select></label>
    <label>Message<textarea name="message" maxlength="8000" rows="4" required dir="auto"></textarea></label>
    <section data-link hidden><label>URL<input name="url" type="url" maxlength="2048" placeholder="https://example.com/news"></label><label>Link button label<input name="buttonText" maxlength="8000" dir="auto"></label></section>
    </fieldset><p role="status" aria-live="polite" data-status></p>`;
  panel.querySelector('[data-player]').textContent = allUsers ? 'All players' : `Player: ${userId}`;
  const type = panel.querySelector('[name=type]');
  const status = panel.querySelector('[data-status]');
  function updateType() {
    panel.querySelector('[data-link]').hidden = type.value !== '3';
    for (const name of ['url', 'buttonText']) { const input = panel.querySelector(`[name="${name}"]`); input.disabled = type.value !== '3'; input.required = type.value === '3'; }
    status.textContent = '';
  }
  type.addEventListener('change', updateType);
  updateType();
  return mountPopupTabs(dialog, nativePanel, panel, {
    nativeLabel: 'Nakama notification', customLabel: 'Game Notification', canSubmit: () => ['2', '3'].includes(type.value), dispose: () => abort.abort(),
    async submit() {
      status.textContent = ''; status.dataset.error = 'false';
      try {
        const payload = buildNotification({ userId, allUsers, type: type.value, message: panel.querySelector('[name=message]').value,
          url: panel.querySelector('[name=url]').value, buttonText: panel.querySelector('[name=buttonText]').value });
        await sendNotification(payload, { fetchFn: (url, options) => fetch(url, { ...options, signal: abort.signal }) });
        if (!panel.isConnected) return;
        type.value = '2'; for (const name of ['message', 'url', 'buttonText']) panel.querySelector(`[name="${name}"]`).value = '';
        updateType(); status.textContent = 'Notification sent. The player’s resources were not changed.';
      } catch (error) { if (!abort.signal.aborted) { status.dataset.error = 'true'; status.textContent = error.message; } }
    },
  });
}
