/**
 * Sort containedIn place values topologically (most-general first, most-specific last).
 *
 * The input `values` is the full transitive closure of containedIn ancestors for a subject:
 * every place in the set is an ancestor (direct or transitive) of the subject.
 *
 * `chainPairs` lists all containedIn relationships *between* the values themselves,
 * including both direct and inferred-transitive pairs (as returned by a SPARQL query
 * against the OWL-inferred endpoint).
 *
 * Algorithm:
 * 1. For each place, compute its depth = count of other places in the set that it is
 *    contained in. Depth 0 = most general (no ancestors in the set), highest depth =
 *    most specific (most ancestors in the set).
 * 2. Sort ascending by depth. Within a depth group, sort alphabetically by label.
 * 3. If a depth group has more than one place, they are fork siblings at the same
 *    level of the hierarchy. The alphabetically-first place is the primary; the
 *    remaining places are annotated as `also: [...]` on the primary entry and are
 *    NOT emitted as separate items.
 *
 * @param {Array<{uri: string, label: string}>} values
 * @param {Array<{child: string, parent: string}>} chainPairs
 * @returns {Array<{uri: string, label: string, also?: Array<{uri: string, label: string}>}>}
 */
export function sortContainedIn(values, chainPairs) {
	if (values.length <= 1) return values;

	// Map from URI → value object
	const uriToValue = new Map(values.map(v => [v.uri, v]));
	const uriSet = new Set(values.map(v => v.uri));

	// For each place, count how many places in the set it is contained in.
	// This count is its depth: 0 = root / most general, higher = more specific.
	const ancestorCount = new Map();
	for (const uri of uriSet) {
		ancestorCount.set(uri, 0);
	}
	for (const { child, parent } of chainPairs) {
		if (uriSet.has(child) && uriSet.has(parent)) {
			ancestorCount.set(child, (ancestorCount.get(child) ?? 0) + 1);
		}
	}

	// Group URIs by depth
	const depthGroups = new Map(); // depth -> uri[]
	for (const uri of uriSet) {
		const d = ancestorCount.get(uri);
		if (!depthGroups.has(d)) depthGroups.set(d, []);
		depthGroups.get(d).push(uri);
	}

	// Sort depth levels ascending (most general first)
	const sortedDepths = [...depthGroups.keys()].sort((a, b) => a - b);

	// For each depth group, sort alphabetically by label and collapse forks
	const result = [];
	for (const d of sortedDepths) {
		const group = depthGroups.get(d);

		// Sort alphabetically by label (non-word chars stripped, as per processBindings)
		group.sort((a, b) => {
			const la = (uriToValue.get(a)?.label || a).replace(/\W/g, '');
			const lb = (uriToValue.get(b)?.label || b).replace(/\W/g, '');
			return la.localeCompare(lb);
		});

		const [primaryUri, ...secondaryUris] = group;
		const primaryValue = { ...uriToValue.get(primaryUri) };

		if (secondaryUris.length > 0) {
			primaryValue.also = secondaryUris.map(u => uriToValue.get(u)).filter(Boolean);
		}

		result.push(primaryValue);
	}

	return result;
}
