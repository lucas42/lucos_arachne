import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fetchWithRetry } from './fetchWithRetry.js';

const URL_ = 'https://eolas.l42.eu/metadata/categories.json';
const opts = (fetchFn, logs = [], sleeps = []) => ({ fetchFn, log: m => logs.push(m), sleep: async ms => { sleeps.push(ms); }, delayMs: 10 });

test('succeeds first time without retry logging', async () => {
	const logs = [];
	const res = await fetchWithRetry(URL_, opts(async () => ({ ok: true, status: 200 }), logs));
	assert.equal(res.status, 200);
	assert.deepEqual(logs, []);
});

test('recovers after transient failures and logs the retries', async () => {
	const results = [{ ok: false, status: 502 }, new Error('ECONNRESET'), { ok: true, status: 200 }];
	const logs = [], sleeps = [];
	const res = await fetchWithRetry(URL_, opts(async () => { const r = results.shift(); if (r instanceof Error) throw r; return r; }, logs, sleeps));
	assert.equal(res.status, 200);
	assert.deepEqual(sleeps, [10, 20]);
	assert.match(logs[0], /Attempt 1 of 3 failed \(HTTP 502\)/);
	assert.match(logs[1], /network error: ECONNRESET/);
	assert.match(logs[2], /succeeded on attempt 3 of 3/);
});

test('fails after bounded attempts naming eolas, attempt count and status', async () => {
	let calls = 0;
	await assert.rejects(
		fetchWithRetry(URL_, opts(async () => { calls++; return { ok: false, status: 502 }; })),
		/eolas\.l42\.eu unreachable after 3 attempts \(last failure: HTTP 502\)/,
	);
	assert.equal(calls, 3);
});
