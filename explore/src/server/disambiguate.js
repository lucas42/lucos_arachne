// Disambiguation logic for search results.
//
// When multiple items share the same pref_label in a result set, this module
// computes a `displayLabel` for each hit that includes a disambiguation suffix.
// Rules applied in order (first match wins):
//
//   1. Different types     → `Label (Type)`
//   2. Same-type Places    → `Label, ContainedIn`   (comma-separated, per spec)
//   3. Same-type Tracks    → `Label (Artist)`
//   4. Fallback            → `Label (Type)`
//
// Disambiguation is ONLY shown when there is a label collision — unique labels
// are displayed as-is.

// Apply disambiguation to an array of Typesense hits.
// Each hit is expected to have at minimum: `document.pref_label` and `document.type`.
// Optional disambiguation fields: `document.contained_in`, `document.artist`.
// Returns a new array with a `displayLabel` property added to each hit.
export function computeDisplayLabels(hits) {
	// Group hits by pref_label to detect collisions.
	const labelGroups = new Map();
	for (const hit of hits) {
		const label = hit.document.pref_label;
		if (!labelGroups.has(label)) labelGroups.set(label, []);
		labelGroups.get(label).push(hit);
	}

	return hits.map(hit => {
		const { pref_label, type, contained_in, artist } = hit.document;
		const group = labelGroups.get(pref_label);

		// No collision — display the label as-is.
		if (group.length <= 1) {
			return { ...hit, displayLabel: pref_label };
		}

		// Rule 1: different types → show the type.
		const types = new Set(group.map(h => h.document.type));
		if (types.size > 1) {
			return { ...hit, displayLabel: `${pref_label} (${type})` };
		}

		// Same-type collision — apply type-specific disambiguation.

		// Rule 2: places with a contained_in label → comma-separated format.
		if (contained_in) {
			return { ...hit, displayLabel: `${pref_label}, ${contained_in}` };
		}

		// Rule 3: items with an artist → bracket format.
		if (artist) {
			return { ...hit, displayLabel: `${pref_label} (${artist})` };
		}

		// Rule 4: fallback — show the type even though it's the same for all.
		return { ...hit, displayLabel: `${pref_label} (${type})` };
	});
}
