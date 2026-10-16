export async function consoleRequest(path, payload, { fetchFn = fetch, session = sessionStorage, local = localStorage, method = 'POST' } = {}) {
  let token;
  try { const saved = session.getItem('SESSION_TOKENS') || local.getItem('SESSION_TOKENS'); token = saved && JSON.parse(saved).accessToken; }
  catch { throw new Error('Your dashboard session is unavailable. Sign in again.'); }
  if (!token) throw new Error('Sign in to the dashboard before sending.');
  const response = await fetchFn(path, {
    method, headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` }, body: method === 'GET' ? undefined : JSON.stringify(payload),
  });
  if (!response.ok) {
    if (response.status === 401) throw Object.assign(new Error('Your dashboard session has expired. Sign in again.'), { requestRejected: true });
    if (response.status === 403) throw Object.assign(new Error('Your dashboard account does not have permission for this operation.'), { requestRejected: true });
    let error;
    try { error = await response.json(); } catch {}
    throw new Error(error?.message || `Request failed (HTTP ${response.status}).`);
  }
  return response;
}

export async function gameRPC(method, payload, options) {
  const response = await consoleRequest(`/v2/console/api/endpoints/rpc/${encodeURIComponent(method)}`, { body: method === 'GET' ? undefined : JSON.stringify(payload), user_id: '' }, options);
  const result = await response.json();
  if (result.error_message) throw new Error(result.error_message);
  let body;
  try { body = JSON.parse(result.body); } catch { throw new Error('The game backend returned an invalid response.'); }
  if (!body.Successful) throw Object.assign(new Error(body.Error?.serverErrorMessage || body.Error?.richMessage?.Message || body.Error?.RichMessage?.Message || 'The operation failed.'), { requestRejected: body.Error?.statusCode >= 400 && body.Error?.statusCode < 500 });
  return body.Payload;
}
