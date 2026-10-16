import { mountNotificationSettings } from './notification-settings.mjs';
import { mountNotificationPopup } from './notification-popup.mjs';
import { mountWalletPopup } from './wallet-popup.mjs';
import { mountPurchasePanel } from './purchase-panel.mjs';

let popup;
let payment;
let settings;
let scheduled = false;

function currentPlayer() { return /^#\/players\/([0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12})\/(notifications|wallet|payments)\/?(?:\?.*)?$/i.exec(location.hash); }

function reconcile() {
  scheduled = false;
  const route = currentPlayer();
  const userId = route?.[1];
  const allUsers = /^#\/notifications\/?(?:\?.*)?$/.test(location.hash);
  const section = allUsers ? 'notifications' : route?.[2];
  const configuration = /^#\/settings\/config\/?(?:\?.*)?$/.test(location.hash);
  if (settings && (!configuration || !settings.card.isConnected)) { settings.dispose(); settings = null; }
  if (configuration && !settings) {
    const heading = [...document.querySelectorAll('main h2')].find(node => node.textContent.startsWith('Server Version:'));
    const card = heading?.closest('.rounded-md');
    if (card) settings = mountNotificationSettings(card);
  }
  if (popup && (popup.userId !== userId || popup.allUsers !== allUsers || popup.section !== section || !popup.dialog.isConnected)) { popup.dispose(); popup = null; }
  if (payment && (payment.userId !== userId || section !== 'payments' || !payment.group.isConnected)) { payment.dispose(); payment = null; }
  if (!userId && !allUsers) return;
  if (!popup && ['notifications', 'wallet'].includes(section)) {
    const heading = section === 'wallet' ? 'Edit user wallet' : 'Send Notification';
    const dialog = [...document.querySelectorAll('form[role="dialog"]')].find(candidate => candidate.querySelector('fieldset')?.firstElementChild?.textContent.trim() === heading);
    if (dialog) {
      let nativePanel;
      if (section === 'notifications') nativePanel = dialog.querySelector('input[name="subject"]')?.closest('label')?.parentElement;
      else nativePanel = dialog.querySelector('.x-scroll')?.firstElementChild?.firstElementChild;
      if (nativePanel) popup = { ...(section === 'notifications' ? mountNotificationPopup(dialog, nativePanel, userId, { allUsers }) : mountWalletPopup(dialog, nativePanel, userId)), userId, section, allUsers };
    }
  }
  if (section === 'payments' && !payment) {
    const buttons = [...document.querySelectorAll('main button, [role="main"] button')];
    const purchases = buttons.find(button => button.textContent.trim() === 'Purchases');
    const subscriptions = buttons.find(button => button.textContent.trim() === 'Subscriptions');
    const group = purchases?.closest('.x-tab-group');
    if (group && group.contains(subscriptions)) payment = mountPurchasePanel(group, [purchases, subscriptions], group.parentElement.parentElement, userId);
  }
}
function schedule() { if (!scheduled) { scheduled = true; requestAnimationFrame(reconcile); } }
new MutationObserver(schedule).observe(document.body, { childList: true, subtree: true });
window.addEventListener('hashchange', schedule);
schedule();
