// Pure function that processes SPARQL result bindings into the data structure
// expected by the item template. Extracted from index.js to allow unit testing.
//
// Accepts an array of SPARQL result row objects (each with predicate, object,
// predicateLabel?, predicateLabelRdfs?, objectLabel?, objectLabelRdfs? bindings).
//
// Returns: { prefLabel, types, predicates, wikipediaLink }

const SKOS_PREF_LABEL = 'http://www.w3.org/2004/02/skos/core#prefLabel';
const RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
const OWL_SAME_AS = 'http://www.w3.org/2002/07/owl#sameAs';
const DBPEDIA_PREFIX = 'http://dbpedia.org/resource/';
const WIKIPEDIA_PREFIX = 'https://en.wikipedia.org/wiki/';

// Pick the best label from two SPARQL bindings (skos:prefLabel preferred over
// rdfs:label; within each, @en or no language tag preferred).
function getBestLabelValue(prefLabelBinding, rdfsLabelBinding) {
	const candidates = [prefLabelBinding, rdfsLabelBinding].filter(Boolean);
	const enCandidate = candidates.find(b => !b['xml:lang'] || b['xml:lang'] === 'en');
	return (enCandidate || candidates[0] || null)?.value || null;
}

// Pick the best label across ALL rows in a group for a given pair of binding keys.
// This is the dedup-safe version: it collects all label bindings from every row
// (which may carry different label values due to SPARQL cross-products) and picks
// the best one using the same preference rules as getBestLabelValue.
function bestLabelAcrossRows(rows, prefKey, rdfsKey) {
	// Prefer skos:prefLabel bindings first, then rdfs:label
	const prefBindings = rows.map(r => r[prefKey]).filter(Boolean);
	const rdfsBindings = rows.map(r => r[rdfsKey]).filter(Boolean);
	const allCandidates = [...prefBindings, ...rdfsBindings];
	if (allCandidates.length === 0) return null;
	const enCandidate = allCandidates.find(b => !b['xml:lang'] || b['xml:lang'] === 'en');
	return (enCandidate || allCandidates[0]).value;
}

export function processBindings(bindings) {
	let prefLabel = null;
	let wikipediaLink = null;
	const types = [];
	const predicates = {};

	// Phase 1: Group all result rows by (predicate URI, object value).
	// SPARQL's left-join OPTIONAL chain produces one row per combination of label
	// values — e.g. 1 skos:prefLabel × 6 rdfs:labels = 6 rows for a single triple.
	// Grouping collapses these back to one entry per unique (predicate, object) pair.
	const groups = new Map(); // key: `${predicate}^A${object}`, value: { predicate, object, rows[] }

	for (const rel of bindings) {
		// The entity's own prefLabel is used as the page title — handle immediately.
		if (rel.predicate.value === SKOS_PREF_LABEL) {
			prefLabel = rel.object.value;
			continue;
		}

		// Wikipedia link derived from owl:sameAs → DBpedia URI.
		if (rel.predicate.value === OWL_SAME_AS && rel.object.value.startsWith(DBPEDIA_PREFIX)) {
			wikipediaLink = rel.object.value.replace(DBPEDIA_PREFIX, WIKIPEDIA_PREFIX);
			// Fall through: owl:sameAs may also be rendered as a predicate if it has a label.
		}

		const key = rel.predicate.value + '\x01' + rel.object.value;
		if (!groups.has(key)) {
			groups.set(key, { predicate: rel.predicate, object: rel.object, rows: [] });
		}
		groups.get(key).rows.push(rel);
	}

	// Phase 2: For each unique (predicate, object) group, pick the best labels
	// across all cross-product rows and produce exactly one rendered entry.
	for (const { predicate, object, rows } of groups.values()) {
		if (predicate.value === RDF_TYPE) {
			const typeLabel = bestLabelAcrossRows(rows, 'objectLabel', 'objectLabelRdfs');
			if (typeLabel) types.push(typeLabel);
			continue;
		}

		const predicateLabelValue = bestLabelAcrossRows(rows, 'predicateLabel', 'predicateLabelRdfs');
		if (!predicateLabelValue) continue;
		if (object.type === 'bnode') continue;

		if (!(predicate.value in predicates)) {
			predicates[predicate.value] = {
				label: predicateLabelValue,
				type: object.type,
				values: [],
			};
		}

		let value;
		switch (object.type) {
			case 'literal':
				value = { label: object.value || 'unknown' };
				break;
			case 'uri':
				value = {
					uri: object.value,
					label: bestLabelAcrossRows(rows, 'objectLabel', 'objectLabelRdfs') || object.value || 'unknown',
				};
				break;
			default:
				throw new Error(`Can't render object type ${object.type}`);
		}
		predicates[predicate.value].values.push(value);
	}

	// Sort each predicate's values alphabetically by label (non-word chars ignored).
	for (const predicate of Object.values(predicates)) {
		predicate.values.sort((a, b) =>
			a.label.replace(/\W/g, '').localeCompare(b.label.replace(/\W/g, ''))
		);
	}

	return { prefLabel, types, predicates, wikipediaLink };
}
