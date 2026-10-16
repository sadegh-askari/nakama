import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildNotification, playerIdFromHash, sendNotification } from './model.mjs';
import { patchStoreProviders } from './build.mjs';

const userId = '11111111-1111-4111-8111-111111111111';
const input = { userId, type: 1, message: 'Already received', rewards: [{ id: 'PowerCore', value: '100' }] };
test('resource payload matches the game and uses only persistent native notifications', () => {
  assert.deepEqual(buildNotification(input), { user_ids: [userId], subject: 'Game notification', code: 17, persistent: true, content: { Value: { Type: 1, Message: { Message: 'Already received' }, Rewards: [{ Id: 'PowerCore', Value: 100 }] } } });
});
test('news and link payloads discard hidden form fields', () => {
  assert.deepEqual(buildNotification({ ...input, type: 2 }).content.Value, { Type: 2, Message: { Message: input.message } });
  assert.deepEqual(buildNotification({ ...input, type: 3, url: 'https://example.com/news', buttonText: 'Read more' }).content.Value, { Type: 3, Message: { Message: input.message }, Url: 'https://example.com/news', ButtonText: { Message: 'Read more' } });
});
test('invalid content and unsafe numbers are rejected', () => {
  for (const patch of [
    { userId: '' }, { userId: '00000000-0000-0000-0000-000000000000' }, { message: ' ' }, { type: 4 }, { rewards: [] },
    { rewards: [{ id: '', value: 1 }] }, { rewards: [{ id: 'Coin', value: 0 }] }, { rewards: [{ id: 'Coin', value: -1 }] },
    { rewards: [{ id: 'Coin', value: 1.5 }] }, { rewards: [{ id: 'Coin', value: '9007199254740992' }] },
    { rewards: [{ id: 'Coin', value: Number.MAX_SAFE_INTEGER }, { id: 'Coin', value: 1 }] },
    { type: 3, url: 'javascript:alert(1)', buttonText: 'Open' }, { type: 3, url: 'https://user:pass@example.com', buttonText: 'Open' },
    { type: 3, url: 'https://example.com', buttonText: '' },
  ]) assert.throws(() => buildNotification({ ...input, ...patch }));
});
test('extension mounts only on a specific player notification route', () => {
  assert.equal(playerIdFromHash(`#/players/${userId}/notifications`), userId);
  for (const route of ['#/notifications', '#/login', `#/players/${userId}/payments`, '#/players/not-a-uuid/notifications']) assert.equal(playerIdFromHash(route), null);
});
test('send uses the native console endpoint and existing session without retries', async () => {
  const payload = buildNotification(input);
  const calls = [];
  await sendNotification(payload, {
    fetchFn: async (...args) => { calls.push(args); return { ok: true }; },
    session: { getItem: () => JSON.stringify({ accessToken: 'fixture-token' }) }, local: { getItem: () => null },
  });
  assert.equal(calls.length, 1);
  assert.equal(calls[0][0], '/v2/console/notification');
  assert.equal(calls[0][1].headers.Authorization, 'Bearer fixture-token');
  assert.deepEqual(JSON.parse(calls[0][1].body), payload);
});
test('authentication, permission and send errors stay visible', async () => {
  for (const status of [401, 403, 500]) {
    let calls = 0;
    await assert.rejects(sendNotification(buildNotification(input), {
      fetchFn: async () => { calls++; return { ok: false, status, json: async () => ({ message: 'failed' }) }; },
      session: { getItem: () => '{"accessToken":"fixture"}' }, local: { getItem: () => null },
    }));
    assert.equal(calls, 1);
  }
  await assert.rejects(sendNotification(buildNotification(input), { session: { getItem: () => null }, local: { getItem: () => null }, fetchFn: () => assert.fail('must not send without a session') }));
});
test('store patch preserves existing mappings and rejects upstream drift', () => {
  const original = 'const stores={APPLE_APP_STORE:0,GOOGLE_PLAY_STORE:1,HUAWEI_APP_GALLERY:2,FACEBOOK_INSTANT_STORE:3,SAMSUNG_GALAXY_STORE:4};';
  assert.match(patchStoreProviders(original), /SAMSUNG_GALAXY_STORE:4,CAFE_BAZAAR:10,MYKET:11/);
  assert.throws(() => patchStoreProviders('changed upstream'));
  assert.throws(() => patchStoreProviders(original + original));
});

test('built assets have synchronized hashed and gzip entry points', async () => {
  const { mkdtemp, readFile, rm } = await import('node:fs/promises');
  const { tmpdir } = await import('node:os');
  const { join } = await import('node:path');
  const { gunzipSync } = await import('node:zlib');
  const { buildConsole } = await import('./build.mjs');
  const out = await mkdtemp(join(tmpdir(), 'nakama-console-test-'));
  try {
    await buildConsole(out);
    const html = await readFile(join(out, 'index.html'), 'utf8');
    assert.match(html, /src="\/custom-console.mjs"/);
    assert.match(html, /href="\/custom-console.css"/);
    assert.equal(gunzipSync(await readFile(join(out, 'index.html.gz'))).toString(), html);
    const entry = html.match(/src="(\/static\/index-cardgame-[a-f0-9]+\.js)"/)[1];
    const js = await readFile(join(out, entry), 'utf8');
    assert.match(js, /CAFE_BAZAAR:10,MYKET:11/);
    assert.equal(gunzipSync(await readFile(join(out, entry + '.gz'))).toString(), js);
  } finally { await rm(out, { recursive: true, force: true }); }
});
test('all-player Game Notifications support only message payloads through native broadcast', () => {
  const message = buildNotification({ allUsers: true, type: 2, message: 'Announcement' });
  assert.deepEqual(message.user_ids, []);
  assert.deepEqual(message.content.Value, { Type: 2, Message: { Message: 'Announcement' } });
  assert.equal(buildNotification({ allUsers: true, type: 3, message: 'Announcement', url: 'https://example.com', buttonText: 'Open' }).content.Value.Type, 3);
  assert.throws(() => buildNotification({ ...input, userId: undefined, allUsers: true }));
  assert.throws(() => buildNotification({ ...input, allUsers: true, type: 2 }));
});
