import { applyNotificationDefault } from './notification-settings.mjs';
import { buildNotification } from './model.mjs';
import { gameRPC } from './api.mjs';
import { createResourcePicker } from './resource-picker.mjs';
import { mountPopupTabs } from './popup-tabs.mjs';

export function mountWalletPopup(dialog, nativePanel, userId) {
  const abort = new AbortController();
  const panel = document.createElement('section');
  panel.className = 'cardgame-notification-panel'; panel.dataset.grantForm = '';
  panel.innerHTML = '<h2>Send Resource</h2><p data-player></p><p class="cardgame-info">Adds resources to the user wallet and sends one resource notification.</p><fieldset disabled><label>Message<textarea name="message" rows="3" maxlength="8000" required dir="auto"></textarea></label><div data-picker></div></fieldset><p role="status" aria-live="polite" data-status></p>';
  panel.querySelector('[data-player]').textContent = `Player: ${userId}`;
  panel.querySelector('[name=message]').value = 'Resources have been added to your account.';
  const picker = createResourcePicker(panel.querySelector('[data-picker]'), { walletOnly: true, signal: abort.signal });
  const status = panel.querySelector('[data-status]');
  const journalKey = `cardgame-admin-grant:${userId}`;
  let pending = null;
  try { pending = JSON.parse(sessionStorage.getItem(journalKey) || 'null'); } catch {}
  let operationId = pending?.OperationId || crypto.randomUUID();
  if (pending) {
    panel.querySelector('[name=message]').value = pending.Message;
    picker.reset(pending.Rewards.map(item => ({ id: item.Id, value: item.Value })));
    status.textContent = 'A previous grant needs confirmation. Submit its restored request to check completion without granting twice.';
  }
  if (!pending) applyNotificationDefault(panel.querySelector('[name=message]'), 'game_resource_grant_message', abort.signal);
  return mountPopupTabs(dialog, nativePanel, panel, {
    nativeLabel: 'Edit wallet', customLabel: 'Send Resource', canSubmit: () => picker.valid(), dispose: () => abort.abort(),
    async submit() {
      status.textContent = ''; status.dataset.error = 'false';
      try {
        const value = buildNotification({ userId, type: 1, message: panel.querySelector('[name=message]').value, rewards: picker.read() }).content.Value;
        const request = { OperationId: operationId, UserId: userId, Message: value.Message.Message, Rewards: value.Rewards };
        if (pending && JSON.stringify(pending) !== JSON.stringify(request)) throw new Error('A previous grant may have completed. Restore its original fields and submit again to check its result before starting another grant.');
        sessionStorage.setItem(journalKey, JSON.stringify(request));
        pending = request;
        const result = await gameRPC('admin/grant_resources', request, { fetchFn: (url, options) => fetch(url, { ...options, signal: abort.signal }) });
        if (!panel.isConnected) return;
        sessionStorage.removeItem(journalKey); operationId = crypto.randomUUID(); pending = null; picker.reset();
        status.textContent = result.AlreadyGranted ? 'This operation was already completed. No resources were added again.' : 'Resources granted, ledger recorded, and notification stored in Nakama.';
      } catch (error) { if (error.requestRejected) { sessionStorage.removeItem(journalKey); pending = null; operationId = crypto.randomUUID(); } if (!abort.signal.aborted) { status.dataset.error = 'true'; status.textContent = error.message; } }
    },
  });
}
