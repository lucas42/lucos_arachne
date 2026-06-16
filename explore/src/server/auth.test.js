import { test, mock } from 'node:test';
import assert from 'node:assert/strict';
import { middleware, parseCookies, hasArachneAccess, _setVerifier } from './auth.js';

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Build a minimal Express-like request object.
 * `protocol` is what Express populates from X-Forwarded-Proto when trust proxy is set.
 */
function makeReq({ cookie, protocol = 'https', originalUrl = '/' } = {}) {
	return {
		headers: {
			host: 'arachne.l42.eu',
			...(cookie !== undefined && { cookie }),
		},
		protocol,
		originalUrl,
	};
}

function makeRes() {
	const res = { auth_agent: undefined };
	res.redirect = mock.fn();
	return res;
}

// ─── parseCookies ─────────────────────────────────────────────────────────────

test('parseCookies: returns empty object for undefined header', () => {
	assert.deepEqual(parseCookies(undefined), {});
});

test('parseCookies: returns empty object for empty string', () => {
	assert.deepEqual(parseCookies(''), {});
});

test('parseCookies: parses a single cookie', () => {
	assert.deepEqual(parseCookies('foo=bar'), { foo: 'bar' });
});

test('parseCookies: parses multiple cookies', () => {
	assert.deepEqual(parseCookies('foo=bar; baz=qux'), { foo: 'bar', baz: 'qux' });
});

test('parseCookies: preserves = within cookie value (e.g. base64 JWT padding)', () => {
	// JWT values are three base64url parts separated by '.'; the signature may end in '='
	assert.deepEqual(
		parseCookies('aithne_session=abc.def.ghi=='),
		{ aithne_session: 'abc.def.ghi==' },
	);
});

test('parseCookies: only splits on the first = in a pair', () => {
	// value itself contains multiple = signs
	assert.deepEqual(parseCookies('k=a=b=c'), { k: 'a=b=c' });
});

test('parseCookies: extracts aithne_session from a multi-cookie header', () => {
	const header = 'other=value; aithne_session=jwt.tok.en==; another=x';
	const result = parseCookies(header);
	assert.equal(result.aithne_session, 'jwt.tok.en==');
	assert.equal(result.other, 'value');
	assert.equal(result.another, 'x');
});

// ─── hasArachneAccess ─────────────────────────────────────────────────────────

// Note: ENVIRONMENT defaults to 'production' in the test process (process.env.ENVIRONMENT
// is not set in the test runner), so render-ui tests use the production path.

test('hasArachneAccess: arachne:read grants access', () => {
	assert.equal(hasArachneAccess(['arachne:read']), true);
});

test('hasArachneAccess: arachne:read alongside other scopes grants access', () => {
	assert.equal(hasArachneAccess(['eolas:read', 'arachne:read', 'webhook']), true);
});

test('hasArachneAccess: empty scopes denies access', () => {
	assert.equal(hasArachneAccess([]), false);
});

test('hasArachneAccess: unrelated scopes deny access', () => {
	assert.equal(hasArachneAccess(['eolas:read', 'webhook']), false);
});

test('hasArachneAccess: render-ui grants access in development', () => {
	const orig = process.env.ENVIRONMENT;
	process.env.ENVIRONMENT = 'development';
	try {
		assert.equal(hasArachneAccess(['render-ui']), true);
	} finally {
		if (orig === undefined) { delete process.env.ENVIRONMENT; } else { process.env.ENVIRONMENT = orig; }
	}
});

test('hasArachneAccess: render-ui denies in production', () => {
	const orig = process.env.ENVIRONMENT;
	process.env.ENVIRONMENT = 'production';
	try {
		assert.equal(hasArachneAccess(['render-ui']), false);
	} finally {
		if (orig === undefined) { delete process.env.ENVIRONMENT; } else { process.env.ENVIRONMENT = orig; }
	}
});

// ─── middleware: redirect path (no JWT verification involved) ─────────────────

test('middleware: no cookie → redirects to aithne login', async () => {
	const req = makeReq();              // no cookie header at all
	const res = makeRes();
	const next = mock.fn();
	await middleware(req, res, next);
	assert.equal(next.mock.calls.length, 0, 'next() must not be called');
	assert.equal(res.redirect.mock.calls.length, 1);
	const [status, url] = res.redirect.mock.calls[0].arguments;
	assert.equal(status, 302);
	assert.ok(url.startsWith('https://aithne.l42.eu/auth/login?next='), `expected login redirect, got: ${url}`);
});

test('middleware: cookie header present but no aithne_session → redirects', async () => {
	const req = makeReq({ cookie: 'some_other_cookie=value' });
	const res = makeRes();
	const next = mock.fn();
	await middleware(req, res, next);
	assert.equal(next.mock.calls.length, 0, 'next() must not be called');
	assert.equal(res.redirect.mock.calls.length, 1);
});

test('middleware: unauthenticated redirect encodes req.protocol into return URL (not hardcoded http)', async () => {
	// Regression guard: the old code used req.query['X-Forwarded-Proto'] which always
	// returned undefined, so return URLs were always http://. With trust proxy set in
	// index.js, req.protocol is the correct value ('https' in production).
	const req = makeReq({ protocol: 'https', originalUrl: '/entities/123?highlight=1' });
	const res = makeRes();
	await middleware(req, res, mock.fn());
	const [, redirectUrl] = res.redirect.mock.calls[0].arguments;
	const returnUrl = decodeURIComponent(new URL(redirectUrl).searchParams.get('next'));
	assert.ok(returnUrl.startsWith('https://'), `return URL should start with https://, got: ${returnUrl}`);
	assert.ok(returnUrl.includes('/explore'), 'return URL must include /explore prefix');
	assert.ok(returnUrl.includes('/entities/123'), 'return URL must preserve originalUrl');
});

// ─── middleware: JWT paths (via _setVerifier seam) ────────────────────────────

test('middleware: valid JWT with arachne:read → calls next() and sets res.auth_agent', async () => {
	const fakePayload = { sub: 'user:1', principal_class: 'human', scopes: ['arachne:read'], exp: 9999999999 };
	_setVerifier(async () => ({ payload: fakePayload }));
	const req = makeReq({ cookie: 'aithne_session=valid.jwt.token' });
	const res = makeRes();
	const next = mock.fn();
	await middleware(req, res, next);
	assert.equal(next.mock.calls.length, 1, 'next() must be called once');
	assert.equal(res.redirect.mock.calls.length, 0, 'must not redirect on success');
	assert.deepEqual(res.auth_agent, fakePayload);
});

test('middleware: valid JWT missing arachne:read → redirects to aithne login', async () => {
	// Valid session but scope not granted — redirect so they can re-auth after grant.
	const fakePayload = { sub: 'user:2', principal_class: 'human', scopes: ['eolas:read'], exp: 9999999999 };
	_setVerifier(async () => ({ payload: fakePayload }));
	const req = makeReq({ cookie: 'aithne_session=valid.jwt.no-scope' });
	const res = makeRes();
	const next = mock.fn();
	await middleware(req, res, next);
	assert.equal(next.mock.calls.length, 0, 'next() must not be called without scope');
	assert.equal(res.redirect.mock.calls.length, 1, 'must redirect when scope missing');
});

test('middleware: valid JWT with empty scopes → redirects (no scope = no access)', async () => {
	const fakePayload = { sub: 'user:3', scopes: [], exp: 9999999999 };
	_setVerifier(async () => ({ payload: fakePayload }));
	const req = makeReq({ cookie: 'aithne_session=valid.jwt.empty-scopes' });
	const res = makeRes();
	const next = mock.fn();
	await middleware(req, res, next);
	assert.equal(next.mock.calls.length, 0);
	assert.equal(res.redirect.mock.calls.length, 1);
});

test('middleware: expired JWT → redirects to aithne login (fail-closed)', async () => {
	_setVerifier(async () => { throw Object.assign(new Error('JWTExpired'), { code: 'ERR_JWT_EXPIRED' }); });
	const req = makeReq({ cookie: 'aithne_session=expired.jwt.token' });
	const res = makeRes();
	const next = mock.fn();
	await middleware(req, res, next);
	assert.equal(next.mock.calls.length, 0, 'next() must not be called on invalid token');
	assert.equal(res.redirect.mock.calls.length, 1, 'must redirect to login on expired token');
});

test('middleware: tampered JWT signature → redirects to aithne login (fail-closed)', async () => {
	_setVerifier(async () => { throw Object.assign(new Error('JWSSignatureVerificationFailed'), { code: 'ERR_JWS_SIGNATURE_VERIFICATION_FAILED' }); });
	const req = makeReq({ cookie: 'aithne_session=tampered.jwt.token' });
	const res = makeRes();
	const next = mock.fn();
	await middleware(req, res, next);
	assert.equal(next.mock.calls.length, 0);
	assert.equal(res.redirect.mock.calls.length, 1);
});
