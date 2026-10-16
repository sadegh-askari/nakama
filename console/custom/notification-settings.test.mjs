import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loadNotificationDefaults, NotificationDefaults } from './notification-settings.mjs';
test('notification defaults load shared native console settings with GET and preserve multiline text', async () => {
 let request;
 const out = await loadNotificationDefaults({ session: { getItem: () => '{"accessToken":"test"}' }, local: { getItem: () => null }, fetchFn: async (url, options) => {
  request = { url, ...options };
  return { ok: true, json: async () => ({ settings: [{ name: 'utc_toggle', value: 'false' }, { name: 'game_purchase_correction_message', value: JSON.stringify('Custom\ncorrection') }] }) };
 } });
 assert.equal(request.url, '/v2/console/setting'); assert.equal(request.method, 'GET'); assert.equal(request.body, undefined);
 assert.equal(out.game_purchase_correction_message, 'Custom\ncorrection'); assert.equal(out.game_resource_grant_message, NotificationDefaults.game_resource_grant_message);
});
