/**
 * Fetches a URL, retrying on network errors and non-2xx responses.
 * Throws an Error naming the attempt count and last failure once attempts are exhausted.
 */
export async function fetchWithRetry(url, { attempts = 3, delayMs = 5000, fetchFn = fetch, sleep = ms => new Promise(r => setTimeout(r, ms)), log = console.log } = {}) {
	let lastFailure;
	for (let attempt = 1; attempt <= attempts; attempt++) {
		try {
			const response = await fetchFn(url);
			if (response.ok) {
				if (attempt > 1) log(`Fetch succeeded on attempt ${attempt} of ${attempts}`);
				return response;
			}
			lastFailure = `HTTP ${response.status}`;
		} catch (err) {
			lastFailure = `network error: ${err.message}`;
		}
		if (attempt < attempts) {
			const wait = delayMs * attempt;
			log(`Attempt ${attempt} of ${attempts} failed (${lastFailure}); retrying in ${wait}ms ...`);
			await sleep(wait);
		}
	}
	throw new Error(`${new URL(url).host} unreachable after ${attempts} attempts (last failure: ${lastFailure}). Re-run the build once eolas is healthy.`);
}
