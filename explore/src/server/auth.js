import { jwtVerify, createRemoteJWKSet } from 'jose';

const AITHNE_ORIGIN = process.env.AITHNE_ORIGIN ?? 'https://aithne.l42.eu';
// AITHNE_JWKS_URL overrides only the server-side key fetch — never iss or login redirect.
// Unset in production (AITHNE_ORIGIN is reachable from containers there).
// In dev, bridge-network containers can't reach the browser-facing localhost address,
// so this points the fetch at a container-reachable host instead.
const AITHNE_JWKS_URL = new URL(process.env.AITHNE_JWKS_URL ?? `${AITHNE_ORIGIN}/.well-known/jwks.json`);
const AITHNE_ISSUER = AITHNE_ORIGIN;
const AITHNE_AUDIENCE = 'l42.eu';
const AITHNE_LOGIN_URL = `${AITHNE_ORIGIN}/auth/login`;

/**
 * Wrap a JWKS GetKeyFunction to serve the last-known-good key on network failure.
 *
 * Caches the most recently returned signing key and falls back to it when a
 * transient network error prevents refreshing the JWKS endpoint. Cold-start
 * (no cached key yet) fails closed — the error propagates normally.
 *
 * Does NOT fall back on ERR_JWKS_NO_MATCHING_KEY: that means the JWKS endpoint
 * was reachable but the kid genuinely isn't there — a wrong-key fallback would
 * just produce a signature mismatch, not a useful result.
 *
 * Mirrors the Python _LKGJWKSClient pattern used in lucos_backups / lucos_contacts.
 * Per lucos_aithne#149 local-verification-contract §1.
 *
 * Exported with underscore prefix for unit testing only — do not call in production code.
 *
 * @param {Function} getRemoteKey - A GetKeyFunction returned by createRemoteJWKSet.
 */
export function _createLKGJWKSet(getRemoteKey) {
	let _lastGoodKey = null;
	return async function (protectedHeader, token) {
		try {
			const key = await getRemoteKey(protectedHeader, token);
			_lastGoodKey = key;
			return key;
		} catch (err) {
			// Genuine key-miss: the endpoint was reachable but the kid isn't in the JWKS.
			// A wrong-key fallback won't help — propagate so the caller rejects the token.
			if (err.code === 'ERR_JWKS_NO_MATCHING_KEY' || err.code === 'ERR_JWKS_MULTIPLE_MATCHING_KEYS') {
				throw err;
			}
			// Network / timeout / parse failure: fall back to last-known-good if available.
			if (_lastGoodKey === null) {
				console.warn('JWKS fetch failed at cold start (no cached key — failing closed):', err.message);
				throw err;
			}
			console.warn('JWKS fetch failed (using last-known-good):', err.message);
			return _lastGoodKey;
		}
	};
}

// JWKS key set with LKG fallback wrapping automatic caching and kid-based rotation.
// _createLKGJWKSet ensures that a brief JWKS blip during key rotation does not
// reject already-valid sessions (tokens signed with the previously-cached key).
let JWKS = _createLKGJWKSet(createRemoteJWKSet(AITHNE_JWKS_URL));

/**
 * Override the module-level JWKS GetKeyFunction. For testing only — do not call in production code.
 */
export function _setJWKS(fn) {
	JWKS = fn;
}

// Internal verify function — replaced in tests via _setVerifier.
let _verifyFn = (token, jwks, opts) => jwtVerify(token, jwks, opts);

/**
 * Override the JWT verifier. For testing only — do not call in production code.
 * Allows unit tests to exercise the middleware without a live JWKS endpoint.
 */
export function _setVerifier(fn) {
	_verifyFn = fn;
}

/**
 * Return true if the JWT scopes array grants access to the arachne /explore UI.
 *
 * ADR-0001 §6: access is granted by named scope, not bare identity.
 * Accepts arachne:read (the canonical scope) for all principals, or render-ui
 * in the development environment as a lucos-ux page-snapshot escape hatch.
 *
 * process.env.ENVIRONMENT is read on every call (not cached at module load) so
 * that tests can control the environment by setting the env var directly.
 */
export function hasArachneAccess(scopes) {
	if (scopes.includes('arachne:read')) return true;
	if ((process.env.ENVIRONMENT ?? 'production') === 'development' && scopes.includes('render-ui')) return true;
	return false;
}

/**
 * Parse a Cookie header string into a key-value object.
 * Splits on '; ' between pairs and on the first '=' only within each pair,
 * so cookie values that contain '=' (e.g. base64-encoded tokens) are preserved.
 */
export function parseCookies(header) {
	if (!header) return {};
	return Object.fromEntries(
		header.split('; ')
			.filter(part => part.includes('='))
			.map(part => {
				const idx = part.indexOf('=');
				return [part.slice(0, idx), part.slice(idx + 1)];
			})
	);
}

/**
 * Provide express middleware function for checking authentication.
 * Reads the aithne_session cookie, verifies the JWT locally via JWKS, and
 * checks for the arachne:read scope (ADR-0001 §6: scope-based access control).
 *
 * Three outcomes:
 *  1. Valid session with arachne:read  → calls next() (access granted)
 *  2. Valid session, missing scope     → 403 access-denied page
 *  3. No session / invalid token       → 302 redirect to aithne login
 *
 * Case 2 must NOT redirect to login: re-authenticating yields the same
 * scopeless token, creating an infinite loop. The resource makes the
 * authorisation decision (ADR-0001 §6); the login page does not.
 */
export async function middleware(req, res, next) {
	const cookies = parseCookies(req.headers.cookie);
	const sessionToken = cookies.aithne_session;

	if (sessionToken) {
		try {
			const { payload } = await _verifyFn(sessionToken, JWKS, {
				issuer: AITHNE_ISSUER,
				audience: AITHNE_AUDIENCE,
				clockTolerance: 30,  // 30-second skew tolerance per aithne local-verification-contract
				algorithms: ['ES256'],  // pin to ES256 — defence-in-depth against algorithm confusion
			});
			if (hasArachneAccess(payload.scopes ?? [])) {
				res.auth_agent = payload;
				return next();
			}
			// Valid session, but principal lacks arachne:read — show access-denied.
			// Redirecting to login here causes an infinite loop: re-authenticating
			// yields the same scopeless token. The user needs a grant, not a re-login.
			console.warn('JWT missing required arachne:read scope:', payload.sub);
			return res.status(403).render('error', {
				title: 'Access denied',
				message: "This action requires the `arachne:read` scope. Contact the administrator to request access.",
			});
		} catch (error) {
			// Invalid or expired token — fall through to login redirect below.
			console.error('JWT verification failed:', error.message);
		}
	}

	// No session cookie, or token failed verification — redirect to aithne login.
	// req.protocol is populated from X-Forwarded-Proto by Express when trust proxy
	// is set (configured in index.js), so this correctly returns 'https' in production.
	// Preserve the existing /explore path-prefix so the user lands back on the
	// correct page after authenticating.
	const returnUrl = `${req.protocol}://${req.headers.host}/explore${req.originalUrl}`;
	return res.redirect(302, `${AITHNE_LOGIN_URL}?next=${encodeURIComponent(returnUrl)}`);
}
