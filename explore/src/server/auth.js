import { jwtVerify, createRemoteJWKSet } from 'jose';

const AITHNE_JWKS_URL = new URL('https://aithne.l42.eu/.well-known/jwks.json');
const AITHNE_ISSUER = 'https://aithne.l42.eu';
const AITHNE_AUDIENCE = 'l42.eu';

// JWKS key set with automatic caching and kid-based rotation support.
// jose's createRemoteJWKSet fetches on first use, caches for 5 minutes,
// and re-fetches when a token's kid is not found in the cache.
const JWKS = createRemoteJWKSet(AITHNE_JWKS_URL);

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
 * Reads the aithne_session cookie and verifies the JWT locally via JWKS.
 * Unauthenticated requests are redirected to the aithne login page.
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
			res.auth_agent = payload;
			return next();
		} catch (error) {
			console.error('JWT verification failed:', error.message);
		}
	}

	// Not authenticated — redirect to aithne login.
	// req.protocol is populated from X-Forwarded-Proto by Express when trust proxy
	// is set (configured in index.js), so this correctly returns 'https' in production.
	// Preserve the existing /explore path-prefix so the user lands back on the
	// correct page after authenticating.
	const returnUrl = `${req.protocol}://${req.headers.host}/explore${req.originalUrl}`;
	return res.redirect(302, `https://aithne.l42.eu/auth/login?next=${encodeURIComponent(returnUrl)}`);
}
