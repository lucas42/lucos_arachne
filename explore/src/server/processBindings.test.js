import { test } from 'node:test';
import assert from 'node:assert/strict';
import { processBindings, processPhaseACounts, processPhaseCBindings } from './processBindings.js';

// ─── helpers ─────────────────────────────────────────────────────────────────

const uri = (value) => ({ type: 'uri', value });
const literal = (value, lang) => lang ? { type: 'literal', value, 'xml:lang': lang } : { type: 'literal', value };
const binding = (predVal, objVal, objType, opts = {}) => ({
	predicate: uri(predVal),
	object: objType === 'literal' ? literal(objVal, opts.objLang) : uri(objVal),
	...(opts.predicateLabel ? { predicateLabel: literal(opts.predicateLabel, opts.predicateLabelLang) } : {}),
	...(opts.predicateLabelRdfs ? { predicateLabelRdfs: literal(opts.predicateLabelRdfs, opts.predicateLabelRdfsLang) } : {}),
	...(opts.objectLabel ? { objectLabel: literal(opts.objectLabel, opts.objectLabelLang) } : {}),
	...(opts.objectLabelRdfs ? { objectLabelRdfs: literal(opts.objectLabelRdfs, opts.objectLabelRdfsLang) } : {}),
});

const CONTAINED_IN = 'https://eolas.l42.eu/ontology/containedIn';
const LONDON = 'https://eolas.l42.eu/metadata/place/2/';
const PREF_LABEL = 'http://www.w3.org/2004/02/skos/core#prefLabel';
const RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
const OWL_SAME_AS = 'http://www.w3.org/2002/07/owl#sameAs';

// ─── deduplication tests ─────────────────────────────────────────────────────

test('deduplicates rows when object has multiple rdfs:labels (the London Zoo bug)', () => {
	// London has 1 skos:prefLabel and 6 rdfs:labels. For the single triple
	// <London Zoo> <containedIn> <London>, SPARQL produces 6 rows (one per rdfs:label).
	const londonRdfsLabels = ['Llundein', 'Londain', 'Londinium', 'London', 'Lunden', 'Trinovantum'];
	const bindings = londonRdfsLabels.map(altName =>
		binding(CONTAINED_IN, LONDON, 'uri', {
			predicateLabel: 'Contained In',
			objectLabel: 'London',       // skos:prefLabel — same across all rows
			objectLabelRdfs: altName,    // rdfs:label — varies per row (the cross-product)
		})
	);

	const { predicates } = processBindings(bindings);

	assert.ok(CONTAINED_IN in predicates, 'predicate should exist');
	assert.equal(predicates[CONTAINED_IN].values.length, 1, 'should have exactly one value, not 6');
	assert.equal(predicates[CONTAINED_IN].values[0].label, 'London', 'should use skos:prefLabel');
});

test('deduplicates rows when predicate has multiple rdfs:labels', () => {
	// A predicate with 1 prefLabel and 3 rdfs:labels produces 3 SPARQL rows for
	// a single triple. After dedup, exactly 1 value should appear.
	const bindings = ['alt1', 'alt2', 'alt3'].map(altName =>
		binding(CONTAINED_IN, LONDON, 'uri', {
			predicateLabel: 'Contained In',
			predicateLabelRdfs: altName,
			objectLabel: 'London',
		})
	);

	const { predicates } = processBindings(bindings);

	assert.equal(predicates[CONTAINED_IN].values.length, 1);
});

test('deduplicates rdf:type rows with multiple object labels', () => {
	// A type object with 3 rdfs:labels causes 3 rows; we should add it to types once.
	const PLACE_TYPE = 'https://eolas.l42.eu/ontology/Place';
	const bindings = ['Place', 'Lieu', 'Ort'].map(altName =>
		binding(RDF_TYPE, PLACE_TYPE, 'uri', {
			objectLabel: 'Place',
			objectLabelRdfs: altName,
		})
	);

	const { types } = processBindings(bindings);

	assert.equal(types.length, 1, 'should add type exactly once');
	assert.equal(types[0], 'Place');
});

// ─── rdfs:label fallback tests (from #427) ───────────────────────────────────

test('uses rdfs:label as fallback when skos:prefLabel is absent on predicate', () => {
	// A predicate from an external ontology (e.g. mo:track) may only have rdfs:label.
	const MO_TRACK = 'http://purl.org/ontology/mo/track';
	const SOME_TRACK = 'https://media-metadata.l42.eu/tracks/123';
	const bindings = [
		binding(MO_TRACK, SOME_TRACK, 'uri', {
			predicateLabelRdfs: 'Track',   // only rdfs:label, no skos:prefLabel
			objectLabel: 'Some Track',
		}),
	];

	const { predicates } = processBindings(bindings);

	assert.ok(MO_TRACK in predicates, 'predicate with rdfs:label fallback should be rendered');
	assert.equal(predicates[MO_TRACK].label, 'Track');
});

test('uses rdfs:label as fallback when skos:prefLabel is absent on object', () => {
	const MO_COMPOSER = 'http://purl.org/ontology/mo/composer';
	const PERSON_URI = 'http://dbpedia.org/resource/Johann_Sebastian_Bach';
	const bindings = [
		binding(MO_COMPOSER, PERSON_URI, 'uri', {
			predicateLabel: 'Composer',
			objectLabelRdfs: 'Johann Sebastian Bach',  // only rdfs:label
		}),
	];

	const { predicates } = processBindings(bindings);

	assert.equal(predicates[MO_COMPOSER].values[0].label, 'Johann Sebastian Bach');
});

// ─── language preference tests ────────────────────────────────────────────────

test('prefers @en label over other languages', () => {
	// A cross-product row set where one rdfs:label is @en and another is @fr.
	// The @en (or no-lang) label should win.
	const bindings = [
		binding(CONTAINED_IN, LONDON, 'uri', {
			predicateLabel: 'Contained In',
			objectLabel: 'London',           // skos:prefLabel @en equivalent (no lang)
			objectLabelRdfs: 'Londre',       // rdfs:label (implicitly @fr-like alternate)
		}),
		binding(CONTAINED_IN, LONDON, 'uri', {
			predicateLabel: 'Contained In',
			objectLabel: 'London',
			objectLabelRdfs: 'Londinium',    // rdfs:label (Latin alternate)
		}),
	];

	const { predicates } = processBindings(bindings);

	assert.equal(predicates[CONTAINED_IN].values[0].label, 'London',
		'should prefer the skos:prefLabel/no-lang label over rdfs:label alts');
});

test('prefers no-language-tag label over @fr when only rdfs:labels available', () => {
	const SOME_PRED = 'http://example.org/prop';
	const SOME_OBJ = 'http://example.org/obj';
	const bindings = [
		// Row 1: rdfs:label with @fr
		binding(SOME_PRED, SOME_OBJ, 'uri', {
			predicateLabel: 'My Prop',
			objectLabelRdfs: 'objet français',
			objectLabelRdfsLang: 'fr',
		}),
		// Row 2: rdfs:label with no language tag
		binding(SOME_PRED, SOME_OBJ, 'uri', {
			predicateLabel: 'My Prop',
			objectLabelRdfs: 'plain label',
		}),
	];

	const { predicates } = processBindings(bindings);

	assert.equal(predicates[SOME_PRED].values[0].label, 'plain label',
		'should prefer the no-lang label over @fr');
});

// ─── prefLabel / Wikipedia / misc ────────────────────────────────────────────

test('extracts entity prefLabel from skos:prefLabel binding', () => {
	const bindings = [
		binding(PREF_LABEL, 'London Zoo', 'literal'),
	];
	const { prefLabel } = processBindings(bindings);
	assert.equal(prefLabel, 'London Zoo');
});

test('extracts Wikipedia link from owl:sameAs → DBpedia URI', () => {
	const bindings = [
		binding(OWL_SAME_AS, 'http://dbpedia.org/resource/London_Zoo', 'uri', {
			predicateLabel: 'same as',
		}),
	];
	const { wikipediaLink } = processBindings(bindings);
	assert.equal(wikipediaLink, 'https://en.wikipedia.org/wiki/London_Zoo');
});

test('returns empty result for empty bindings', () => {
	const { prefLabel, types, predicates, wikipediaLink } = processBindings([]);
	assert.equal(prefLabel, null);
	assert.deepEqual(types, []);
	assert.deepEqual(predicates, {});
	assert.equal(wikipediaLink, null);
});

test('ignores predicates that have no label (not skos:prefLabel or rdfs:label)', () => {
	const SOME_PRED = 'http://example.org/unlabelled';
	const bindings = [
		binding(SOME_PRED, 'http://example.org/val', 'uri', {
			objectLabel: 'Some value',
			// No predicateLabel or predicateLabelRdfs
		}),
	];
	const { predicates } = processBindings(bindings);
	assert.ok(!(SOME_PRED in predicates), 'predicate without a label should not be rendered');
});

test('sorts values within a predicate alphabetically', () => {
	const SOME_PRED = 'http://example.org/has';
	const bindings = [
		binding(SOME_PRED, 'http://example.org/z', 'uri', {
			predicateLabel: 'Has',
			objectLabel: 'Zebra',
		}),
		binding(SOME_PRED, 'http://example.org/a', 'uri', {
			predicateLabel: 'Has',
			objectLabel: 'Antelope',
		}),
		binding(SOME_PRED, 'http://example.org/m', 'uri', {
			predicateLabel: 'Has',
			objectLabel: 'Meerkat',
		}),
	];
	const { predicates } = processBindings(bindings);
	const labels = predicates[SOME_PRED].values.map(v => v.label);
	assert.deepEqual(labels, ['Antelope', 'Meerkat', 'Zebra']);
});

// ─── processPhaseACounts tests ────────────────────────────────────────────────

test('processPhaseACounts: returns count and label for a simple predicate', () => {
	const bindings = [
		{
			p: { type: 'uri', value: CONTAINED_IN },
			pLabel: literal('Contained In'),
			count: literal('42'),
		},
	];
	const counts = processPhaseACounts(bindings);
	assert.equal(counts.get(CONTAINED_IN).count, 42);
	assert.equal(counts.get(CONTAINED_IN).label, 'Contained In');
});

test('processPhaseACounts: collapses multiple GROUP BY rows for same predicate (label cross-product)', () => {
	// A predicate with 2 rdfs:label values produces 2 GROUP BY rows, each with count=7.
	const bindings = [
		{ p: { type: 'uri', value: CONTAINED_IN }, pLabelRdfs: literal('ContainedIn'), count: literal('7') },
		{ p: { type: 'uri', value: CONTAINED_IN }, pLabelRdfs: literal('Is inside'), count: literal('7') },
	];
	const counts = processPhaseACounts(bindings);
	assert.equal(counts.size, 1, 'should collapse to a single predicate entry');
	assert.equal(counts.get(CONTAINED_IN).count, 7);
});

test('processPhaseACounts: prefers skos:prefLabel over rdfs:label', () => {
	const bindings = [
		{
			p: { type: 'uri', value: CONTAINED_IN },
			pLabel: literal('Contained In'),
			pLabelRdfs: literal('is inside'),
			count: literal('5'),
		},
	];
	const counts = processPhaseACounts(bindings);
	assert.equal(counts.get(CONTAINED_IN).label, 'Contained In');
});

test('processPhaseACounts: returns null label when no label bindings present', () => {
	const bindings = [
		{ p: { type: 'uri', value: CONTAINED_IN }, count: literal('3') },
	];
	const counts = processPhaseACounts(bindings);
	assert.equal(counts.get(CONTAINED_IN).label, null);
});

test('processPhaseACounts: handles multiple distinct predicates', () => {
	const SOME_PRED = 'http://example.org/prop';
	const bindings = [
		{ p: { type: 'uri', value: CONTAINED_IN }, pLabel: literal('Contained In'), count: literal('5') },
		{ p: { type: 'uri', value: SOME_PRED }, pLabel: literal('My Prop'), count: literal('100') },
	];
	const counts = processPhaseACounts(bindings);
	assert.equal(counts.size, 2);
	assert.equal(counts.get(CONTAINED_IN).count, 5);
	assert.equal(counts.get(SOME_PRED).count, 100);
});

test('processPhaseACounts: returns empty Map for empty bindings', () => {
	const counts = processPhaseACounts([]);
	assert.equal(counts.size, 0);
});

// ─── processPhaseCBindings tests ─────────────────────────────────────────────

test('processPhaseCBindings: returns sorted values for URI objects', () => {
	const bindings = [
		{ object: { type: 'uri', value: 'http://example.org/zebra' }, objectLabel: literal('Zebra') },
		{ object: { type: 'uri', value: 'http://example.org/ant' }, objectLabel: literal('Ant') },
		{ object: { type: 'uri', value: 'http://example.org/meerkat' }, objectLabel: literal('Meerkat') },
	];
	const values = processPhaseCBindings(bindings);
	const labels = values.map(v => v.label);
	assert.deepEqual(labels, ['Ant', 'Meerkat', 'Zebra']);
});

test('processPhaseCBindings: deduplicates rows for same object URI (label cross-product)', () => {
	// Same object URI appears twice with different label variants.
	const bindings = [
		{ object: { type: 'uri', value: LONDON }, objectLabel: literal('London'), objectLabelRdfs: literal('Londinium') },
		{ object: { type: 'uri', value: LONDON }, objectLabel: literal('London'), objectLabelRdfs: literal('Llundein') },
	];
	const values = processPhaseCBindings(bindings);
	assert.equal(values.length, 1, 'should deduplicate to a single entry');
	assert.equal(values[0].label, 'London');
});

test('processPhaseCBindings: skips blank nodes', () => {
	const bindings = [
		{ object: { type: 'bnode', value: 'b0' } },
		{ object: { type: 'uri', value: 'http://example.org/real' }, objectLabel: literal('Real') },
	];
	const values = processPhaseCBindings(bindings);
	assert.equal(values.length, 1, 'blank node should be filtered out');
	assert.equal(values[0].label, 'Real');
});

test('processPhaseCBindings: uses object URI as fallback label when no label present', () => {
	const SOME_URI = 'http://example.org/unlabelled';
	const bindings = [
		{ object: { type: 'uri', value: SOME_URI } },
	];
	const values = processPhaseCBindings(bindings);
	assert.equal(values[0].label, SOME_URI);
	assert.equal(values[0].uri, SOME_URI);
});

test('processPhaseCBindings: handles literal objects', () => {
	const bindings = [
		{ object: { type: 'literal', value: 'Hello world' } },
	];
	const values = processPhaseCBindings(bindings);
	assert.equal(values.length, 1);
	assert.equal(values[0].label, 'Hello world');
	assert.ok(!values[0].uri, 'literals should not have a uri property');
});

test('processPhaseCBindings: returns empty array for empty bindings', () => {
	const values = processPhaseCBindings([]);
	assert.deepEqual(values, []);
});
