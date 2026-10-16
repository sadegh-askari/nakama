import { test } from 'node:test';
import assert from 'node:assert/strict';
import { gameRPC } from './api.mjs';
const session = { getItem: () => '{"accessToken":"fixture"}' };
const local = { getItem: () => null };
test('admin RPC never impersonates a player and unwraps the game response', async () => {
 let call;
 const out = await gameRPC('admin/grant_resources', { UserId: 'selected', OperationId: 'stable' }, { session, local, fetchFn: async (...args) => { call = args; return { ok: true, json: async () => ({ body: '{"Successful":true,"Payload":{"AlreadyGranted":true}}' }) }; } });
 assert.equal(out.AlreadyGranted, true);
 assert.equal(call[0], '/v2/console/api/endpoints/rpc/admin%2Fgrant_resources');
 assert.deepEqual(JSON.parse(call[1].body), { user_id: '', body: '{"UserId":"selected","OperationId":"stable"}' });
});
test('uncertain failures preserve the operation ID while pre-dispatch rejections allow a correction', async () => {
 for (const [statusCode, rejected] of [[400,true],[500,false]]) {
  await assert.rejects(gameRPC('admin/grant_resources', {}, { session, local, fetchFn: async () => ({ ok: true, json: async () => ({ body: JSON.stringify({ Successful: false, Error: { statusCode, serverErrorMessage: 'visible error' } }) }) }) }), error => error.requestRejected === rejected && error.message === 'visible error');
 }
 await assert.rejects(gameRPC('admin/grant_resources', {}, { session, local, fetchFn: async () => { throw new Error('lost response'); } }), error => !error.requestRejected);
});
