import querystring from 'querystring';
let agents = {}; // Local cache of agent data, keyed by authenication token

/**
 * Checks whether a given token is authenticated to access the service
 * @returns Promise{boolean}
 */
export async function isAuthenticated(token) {
	if (!token) return false;

	// If we've already validated the given token before, approve immediately
	if (agents[token]) return true;

	// Otherwise, verify it against the authentication service
	const authurl = 'https://auth.l42.eu/data?' + querystring.stringify({ token });
	try {
		const auth_resp = await fetch(authurl);
		if (auth_resp.status !== 200) throw new Error(`Bad Status Code from auth server ${auth_resp.status}`);
		agents[token] = await auth_resp.json(); // Cache the data locally, so we don't need to make a call for this token in future
		return true;
	} catch (error) {
		console.error("Failed to auth ", error);
		return false;
	}
}

/**
 * Provide express middleware function for checking authentication
 */
export async function middleware(req, res, next) {
	const cookies = querystring.parse(req.headers.cookie, '; ');

	// Token in GET parameter takes precedence over cookie.
	// This allows for a case where the cookie has a bad token, but the user has just returned from the authentication service with a fresh one
	// It should also support useragents which don't have cookies (though the user will have to hit the auth service between each new page)
	const token = req.query.token || cookies.auth_token;

	if (await isAuthenticated(token)) {
		res.auth_agent = agents[token];
		if (cookies.auth_token !== token) res.cookie('auth_token', token);
		next();
	} else {

		// If no token was given, or the token wasn't successfully verified, send the user to the authentication service to log in
		const protocol = req.query['X-Forwarded-Proto'] || 'http';
		return res.redirect(302, "https://auth.l42.eu/authenticate?redirect_uri="+encodeURIComponent(protocol+'://'+req.headers.host+req.originalUrl));
	}
}
