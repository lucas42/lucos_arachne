// Pure helper functions for owl:sameAs closure handling on the item page.
// Extracted from index.js so they can be unit-tested without a running triplestore.
//
// The complementary SPARQL-calling functions (getSameAsClosure, getPrefIdPairs)
// live in index.js alongside sparqlFetch.

export const OWL_SAME_AS = 'http://www.w3.org/2002/07/owl#sameAs';
export const PREFERRED_IDENTIFIER = 'https://eolas.l42.eu/ontology/preferredIdentifier';

/**
 * Given an array of closure URIs and a Map of preferredIdentifier edges
 * (source → target, already filtered to URIs within the closure), walk the
 * chain to find the terminal URI — the one with no outgoing edge within the
 * closure.  That terminal is the primary URI.
 *
 * Falls back to lexicographic min when no edges exist (deterministic, same
 * rule as #539's Python _find_primary_uri for the search index).
 *
 * @param {string[]} closureUris  - all URIs in the owl:sameAs closure
 * @param {Map<string,string>} prefIdMap - preferredIdentifier source → target
 * @returns {string} the primary URI
 */
export function findPrimaryUri(closureUris, prefIdMap) {
	const closureSet = new Set(closureUris);

	// Filter to edges that stay within the closure
	const edgesInClosure = new Map(
		[...prefIdMap.entries()].filter(([s, t]) => closureSet.has(s) && closureSet.has(t)),
	);

	if (edgesInClosure.size === 0) {
		// Deterministic fallback: lexicographic min
		return closureUris.slice().sort()[0];
	}

	// Roots: URIs that are NOT the target of any in-closure edge
	const targets = new Set(edgesInClosure.values());
	const roots = closureUris.filter(u => !targets.has(u)).sort();
	const start = roots.length > 0 ? roots[0] : closureUris.slice().sort()[0];

	// Walk the chain from the root to the terminal
	const visited = new Set();
	let current = start;
	while (true) {
		if (visited.has(current)) break; // cycle safety — shouldn't happen with asymmetric property
		visited.add(current);
		const next = edgesInClosure.get(current);
		if (next === undefined || !closureSet.has(next)) return current;
		current = next;
	}
	return current; // cycle hit — return last node visited
}

/**
 * Returns true when the item page should 302-redirect the requested URI to
 * the primary URI.
 *
 * Redirect logic: redirect iff preferredIdentifier edges exist within the
 * closure AND the requested URI is not the primary.  When no edges exist the
 * closure has no deterministic canonical URL so we show the requested URI
 * as-is (de-facto primary per the issue spec).
 *
 * @param {string} requestedUri
 * @param {string} primaryUri
 * @param {Map<string,string>} prefIdMap - preferredIdentifier source → target
 * @param {string[]} closureUris
 * @returns {boolean}
 */
export function shouldRedirectToPrimary(requestedUri, primaryUri, prefIdMap, closureUris) {
	if (requestedUri === primaryUri) return false;
	const closureSet = new Set(closureUris);
	const hasEdgesInClosure = [...prefIdMap.entries()].some(
		([s, t]) => closureSet.has(s) && closureSet.has(t),
	);
	return hasEdgesInClosure;
}

/**
 * Build the ordered list of { uri, label } objects for the "View/Edit Item" links.
 *
 * - Single-URI closure: one entry with label "View/Edit Item".
 * - Multi-URI closure: one entry per URI, primary first, others sorted alphabetically,
 *   with label "View/Edit Item on {hostname}" for each.
 *
 * @param {string} primaryUri
 * @param {string[]} closureUris
 * @returns {{ uri: string, label: string }[]}
 */
export function buildClosureLinks(primaryUri, closureUris) {
	const sorted = [primaryUri, ...closureUris.filter(u => u !== primaryUri).sort()];
	if (sorted.length === 1) {
		return [{ uri: sorted[0], label: 'View/Edit Item' }];
	}
	return sorted.map(u => ({
		uri: u,
		label: `View/Edit Item on ${new URL(u).hostname}`,
	}));
}

/**
 * Strip owl:sameAs and preferredIdentifier values that are URIs of closure
 * members from the rendered predicates.  Removes the whole predicate entry if
 * no values remain after filtering.  Non-closure values (e.g. a DBpedia URI
 * in owl:sameAs) are kept.
 *
 * Mutates the predicates object in place.
 *
 * @param {Object} predicates - the predicates map produced by processBindings
 * @param {string[]} closureUris
 */
export function filterClosurePredicates(predicates, closureUris) {
	if (closureUris.length <= 1) return; // single-URI closure — nothing to filter
	const closureSet = new Set(closureUris);
	const PLUMBING_PREDS = [OWL_SAME_AS, PREFERRED_IDENTIFIER];

	for (const predUri of PLUMBING_PREDS) {
		if (!(predUri in predicates)) continue;
		const pred = predicates[predUri];
		// Keep only values whose URI is outside the closure (e.g. DBpedia link)
		pred.values = pred.values.filter(v => !v.uri || !closureSet.has(v.uri));
		if (pred.values.length === 0) {
			delete predicates[predUri];
		}
	}
}
