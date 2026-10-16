import { applyNotificationDefault } from './notification-settings.mjs';
import { gameRPC } from './api.mjs';
import { setCurrencyLabel } from './currency-art.mjs';

export function mountPurchasePanel(group, nativeButtons, tabWrapper, userId) {
  const abort = new AbortController();
  const contentHost = tabWrapper.parentElement;
  const nativeGroup = nativeButtons[0].parentElement;
  const button = document.createElement('button');
  button.type = 'button'; button.className = nativeButtons[1].className + ' cardgame-correction-tab';
  const label = document.createElement('div'); label.className = 'flex items-center justify-center';
  const text = document.createElement('div'); text.className = 'font-medium'; text.textContent = 'Purchase Correction'; label.append(text); button.append(label);
  nativeGroup.append(button);
  const panel = document.createElement('section');
  panel.className = 'cardgame-notification-panel cardgame-purchase-panel'; panel.dataset.correctionForm = ''; panel.hidden = true;
  panel.innerHTML = `<h2>Purchase Correction</h2><p>Verify the token and its owner before correcting an undelivered purchase.</p><form>
    <fieldset><label>Purchase token<input name="token" maxlength="512" required autocomplete="off" spellcheck="false"></label>
    <label>Store<select name="store" aria-label="Store"><option value="10">Bazaar</option><option value="11">Myket</option><option value="1">Google Play</option></select></label>
    <label>Correction notification message<textarea name="message" rows="3" maxlength="8000" dir="auto"></textarea></label>
    <button type="submit" class="cardgame-primary">Verify purchase</button><button type="button" data-correct class="cardgame-primary" hidden>Correct Purchase</button></fieldset>
    <p data-status role="status" aria-live="polite"></p><ul data-rewards></ul></form>`;
  panel.querySelector('[name=message]').value = 'A purchase correction has been issued to you.';
  contentHost.append(panel);
  applyNotificationDefault(panel.querySelector('[name=message]'), 'game_purchase_correction_message', abort.signal);
  const status = panel.querySelector('[data-status]');
  const correct = panel.querySelector('[data-correct]');
  const fields = panel.querySelector('fieldset');
  const hiddenNative = new Set();
  const track = nativeGroup.nextElementSibling;
  const customTrack = track?.cloneNode(false);
  if (customTrack) { customTrack.classList.add('cardgame-correction-indicator'); customTrack.setAttribute('aria-hidden', 'true'); customTrack.hidden = true; track.after(customTrack); }
  let customSelected = false;
  let nativeSelection = [];
  const originalTrackVisibility = track?.style.visibility || '';
  function positionTrack() { if (customTrack && customSelected) { customTrack.style.left = `${button.offsetLeft}px`; customTrack.style.width = `${button.offsetWidth}px`; } }
  const resize = new ResizeObserver(positionTrack);
  [nativeGroup, button, ...nativeButtons].forEach(element => resize.observe(element));
  let verified = null;
  function show(custom) {
    if (custom && !customSelected) nativeSelection = nativeButtons.map(native => native.getAttribute('aria-selected'));
    if (!custom && customSelected) nativeButtons.forEach((native, index) => { if (nativeSelection[index] === null) native.removeAttribute('aria-selected'); else native.setAttribute('aria-selected', nativeSelection[index]); });
    customSelected = custom;
    if (custom) nativeButtons.forEach(native => native.setAttribute('aria-selected', 'false'));
    button.setAttribute('aria-selected', String(custom));
    const content = [...contentHost.children].filter(child => child !== tabWrapper && child !== panel);
    content.forEach(child => { child.hidden = custom; child.classList.add('cardgame-native-payment'); hiddenNative.add(child); });
    group.classList.toggle('cardgame-payment-custom-selected', custom);
    button.setAttribute('aria-pressed', String(custom)); panel.hidden = !custom;
    if (track) track.style.visibility = custom ? 'hidden' : originalTrackVisibility;
    if (customTrack) customTrack.hidden = !custom;
    positionTrack();
  }
  const nativeClick = () => show(false);
  nativeButtons.forEach(native => native.addEventListener('click', nativeClick));
  button.addEventListener('click', () => show(true));
  function request() { return { UserId: userId, Token: panel.querySelector('[name=token]').value.trim(), Store: Number(panel.querySelector('[name=store]').value) }; }
  function invalidate() { verified = null; correct.hidden = true; status.textContent = ''; panel.querySelector('[data-rewards]').replaceChildren(); }
  for (const name of ['token', 'store']) panel.querySelector(`[name="${name}"]`).addEventListener('input', invalidate);
  function display(result) {
    status.dataset.error = String(!['not_delivered', 'already_received', 'corrected'].includes(result.Status));
    status.textContent = result.Message;
    correct.hidden = !result.CanCorrect;
    const list = panel.querySelector('[data-rewards]'); list.replaceChildren();
    for (const reward of result.Rewards || []) { const row = document.createElement('li'); setCurrencyLabel(row, reward.Id, `${reward.Id}: ${reward.Value}`); list.append(row); }
  }
  panel.querySelector('form').addEventListener('submit', async event => {
    event.preventDefault(); if (fields.disabled) return;
    invalidate(); fields.disabled = true;
    try {
      const req = request();
      const result = await gameRPC('admin/purchase_verify', req, { fetchFn: (url, options) => fetch(url, { ...options, signal: abort.signal }) });
      if (abort.signal.aborted) return;
      display(result); if (result.CanCorrect) verified = JSON.stringify(req);
    } catch (error) { if (!abort.signal.aborted) { status.dataset.error = 'true'; status.textContent = error.message; } }
    finally { fields.disabled = false; }
  });
  correct.addEventListener('click', async () => {
    if (fields.disabled || verified !== JSON.stringify(request())) return;
    const message = panel.querySelector('[name=message]').value.trim();
    if (!message) { status.textContent = 'Enter the correction notification message.'; return; }
    fields.disabled = true; correct.hidden = true;
    try {
      const result = await gameRPC('admin/purchase_correct', { ...request(), Message: message }, { fetchFn: (url, options) => fetch(url, { ...options, signal: abort.signal }) });
      if (!abort.signal.aborted) { display(result); verified = null; }
    } catch (error) { if (!abort.signal.aborted) { status.dataset.error = 'true'; status.textContent = error.message; } }
    finally { fields.disabled = false; }
  });
  return { group, userId, dispose() { abort.abort(); resize.disconnect(); nativeButtons.forEach(native => native.removeEventListener('click', nativeClick)); show(false); hiddenNative.forEach(child => { child.hidden = false; child.classList.remove('cardgame-native-payment'); }); customTrack?.remove(); button.remove(); panel.remove(); } };
}
