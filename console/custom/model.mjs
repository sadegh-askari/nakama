import { consoleRequest } from './api.mjs';
export const StoreProviders = Object.freeze({ CAFE_BAZAAR: 10, MYKET: 11 });
export const GameNotificationCode = 17;
export const NotificationTypes = Object.freeze({ Resources: 1, Message: 2, LinkedMessage: 3 });

export function playerIdFromHash(hash) {
  return /^#\/players\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\/notifications\/?(?:\?.*)?$/i.exec(hash)?.[1] ?? null;
}

export function buildNotification({ userId, allUsers = false, type, message, rewards = [], url = '', buttonText = '' }) {
  if (allUsers && (userId || Number(type) === NotificationTypes.Resources)) throw new Error('All-user game notifications support messages only.');
  if (!allUsers && (!userId || userId === '00000000-0000-0000-0000-000000000000' || !/^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i.test(userId))) throw new Error('Select a valid player.');
  const messageText = message.trim();
  if (!messageText || messageText.length > 8000) throw new Error('Enter a message of up to 8000 characters.');
  const value = { Type: Number(type), Message: { Message: messageText } };
  switch (value.Type) {
    case NotificationTypes.Resources:
      if (rewards.length < 1 || rewards.length > 100) throw new Error('Add between 1 and 100 resources.');
      const totals = new Map();
      value.Rewards = rewards.map(item => {
        const id = item.id.trim();
        const amount = Number(item.value);
        const total = (totals.get(id) ?? 0) + amount;
        if (!id || !Number.isSafeInteger(amount) || amount <= 0 || !Number.isSafeInteger(total)) throw new Error('Each resource needs an ID and a positive whole-number value within the safe integer range.');
        totals.set(id, total);
        return { Id: id, Value: amount };
      });
      break;
    case NotificationTypes.Message:
      break;
    case NotificationTypes.LinkedMessage:
      let link;
      try { link = new URL(url.trim()); } catch { throw new Error('Enter a valid HTTP(S) URL.'); }
      if (!['http:', 'https:'].includes(link.protocol) || !link.hostname || link.username || link.password || url.trim().length > 2048) throw new Error('Enter an HTTP(S) URL without embedded credentials.');
      const label = buttonText.trim();
      if (!label || label.length > 8000) throw new Error('Enter a link button label of up to 8000 characters.');
      value.Url = url.trim();
      value.ButtonText = { Message: label };
      break;
    default:
      throw new Error('Select a notification type.');
  }
  return {
    user_ids: allUsers ? [] : [userId],
    subject: 'Game notification',
    code: GameNotificationCode,
    persistent: true,
    content: { Value: value },
  };
}

export async function sendNotification(payload, options) {
  await consoleRequest('/v2/console/notification', payload, options);
}
