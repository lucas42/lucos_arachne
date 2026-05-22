import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
	findPrimaryUri,
	shouldRedirectToPrimary,
	filterClosurePredicates,
	OWL_SAME_AS,
	PREFERRED_IDENTIFIER,
} from './sameAsHelpers.js';

// ─── findPrimaryUri ───────────────────────────────────────────────────────────

test('findPrimaryUri: returns lexicographic min when no preferredIdentifier edges', () => {
	const uris = ['https://contacts.example/2', 'https://eolas.example/1', 'https://eolas.example/3'];
	const result = findPrimaryUri(uris, new Map());
	assert.equal(result, 'https://contacts.example/2');
});

test('findPrimaryUri: single-URI closure returns that URI', () => {
	const result = findPrimaryUri(['https://eolas.example/1'], new Map());
	assert.equal(result, 'https://eolas.example/1');
});

test('findPrimaryUri: walks preferredIdentifier to terminal (two-member closure)', () => {
	// contacts URI preferredIdentifier→ eolas URI, so eolas is primary
	const uris = ['https://contacts.example/person/1', 'https://eolas.example/person/1'];
	const prefIdMap = new Map([['https://contacts.example/person/1', 'https://eolas.example/person/1']]);
	const result = findPrimaryUri(uris, prefIdMap);
	assert.equal(result, 'https://eolas.example/person/1');
});

test('findPrimaryUri: walks preferredIdentifier chain to terminal (three-member)', () => {
	// A → B → C chain; C is the primary (terminal)
	const uris = ['https://a.example/', 'https://b.example/', 'https://c.example/'];
	const prefIdMap = new Map([
		['https://a.example/', 'https://b.example/'],
		['https://b.example/', 'https://c.example/'],
	]);
	const result = findPrimaryUri(uris, prefIdMap);
	assert.equal(result, 'https://c.example/');
});

test('findPrimaryUri: ignores preferredIdentifier edges to URIs outside the closure', () => {
	// Edge points outside closure — treated as no in-closure edges → fallback
	const uris = ['https://a.example/', 'https://b.example/'];
	const prefIdMap = new Map([['https://a.example/', 'https://external.example/']]);
	const result = findPrimaryUri(uris, prefIdMap);
	// No in-closure edge → lexicographic min
	assert.equal(result, 'https://a.example/');
});

test('findPrimaryUri: cycle safety — does not loop forever', () => {
	// A → B → A (cycle) — should return some URI without hanging
	const uris = ['https://a.example/', 'https://b.example/'];
	const prefIdMap = new Map([
		['https://a.example/', 'https://b.example/'],
		['https://b.example/', 'https://a.example/'],
	]);
	const result = findPrimaryUri(uris, prefIdMap);
	// Should return one of the two without throwing or hanging
	assert.ok(uris.includes(result), 'should return a URI from the closure');
});

// ─── shouldRedirectToPrimary ──────────────────────────────────────────────────

test('shouldRedirectToPrimary: returns false when requested URI equals primary', () => {
	const uris = ['https://a.example/', 'https://b.example/'];
	const prefIdMap = new Map([['https://b.example/', 'https://a.example/']]);
	const result = shouldRedirectToPrimary('https://a.example/', 'https://a.example/', prefIdMap, uris);
	assert.equal(result, false);
});

test('shouldRedirectToPrimary: returns true for secondary URI with edges in closure', () => {
	const uris = ['https://a.example/', 'https://b.example/'];
	const prefIdMap = new Map([['https://b.example/', 'https://a.example/']]);
	const result = shouldRedirectToPrimary('https://b.example/', 'https://a.example/', prefIdMap, uris);
	assert.equal(result, true);
});

test('shouldRedirectToPrimary: returns false when no preferredIdentifier edges in closure', () => {
	// No canonical primary → no redirect even for the non-min URI
	const uris = ['https://a.example/', 'https://b.example/'];
	const result = shouldRedirectToPrimary('https://b.example/', 'https://a.example/', new Map(), uris);
	assert.equal(result, false);
});

test('shouldRedirectToPrimary: returns false for single-URI closure (no edges)', () => {
	const uris = ['https://a.example/'];
	const result = shouldRedirectToPrimary('https://a.example/', 'https://a.example/', new Map(), uris);
	assert.equal(result, false);
});

// ─── filterClosurePredicates ──────────────────────────────────────────────────

test('filterClosurePredicates: removes owl:sameAs predicate when all values are closure members', () => {
	const closureUris = ['https://a.example/', 'https://b.example/'];
	const predicates = {
		[OWL_SAME_AS]: {
			label: 'same as',
			type: 'uri',
			values: [
				{ uri: 'https://b.example/', label: 'B' },
			],
		},
	};
	filterClosurePredicates(predicates, closureUris);
	assert.ok(!(OWL_SAME_AS in predicates), 'owl:sameAs should be removed');
});

test('filterClosurePredicates: removes preferredIdentifier predicate when all values are closure members', () => {
	const closureUris = ['https://a.example/', 'https://b.example/'];
	const predicates = {
		[PREFERRED_IDENTIFIER]: {
			label: 'preferred identifier',
			type: 'uri',
			values: [
				{ uri: 'https://b.example/', label: 'B' },
			],
		},
	};
	filterClosurePredicates(predicates, closureUris);
	assert.ok(!(PREFERRED_IDENTIFIER in predicates), 'preferredIdentifier should be removed');
});

test('filterClosurePredicates: keeps owl:sameAs entry when it has non-closure values (e.g. DBpedia link)', () => {
	const closureUris = ['https://a.example/', 'https://b.example/'];
	const predicates = {
		[OWL_SAME_AS]: {
			label: 'same as',
			type: 'uri',
			values: [
				{ uri: 'https://b.example/', label: 'B' },        // closure member — filtered
				{ uri: 'http://dbpedia.org/resource/A', label: 'A (DBpedia)' }, // outside — kept
			],
		},
	};
	filterClosurePredicates(predicates, closureUris);
	assert.ok(OWL_SAME_AS in predicates, 'owl:sameAs should be kept (has DBpedia value)');
	assert.equal(predicates[OWL_SAME_AS].values.length, 1);
	assert.equal(predicates[OWL_SAME_AS].values[0].uri, 'http://dbpedia.org/resource/A');
});

test('filterClosurePredicates: does not touch other predicates', () => {
	const closureUris = ['https://a.example/', 'https://b.example/'];
	const SOME_PRED = 'http://example.org/somePred';
	const predicates = {
		[SOME_PRED]: {
			label: 'some predicate',
			type: 'uri',
			values: [{ uri: 'https://b.example/', label: 'B' }],
		},
	};
	filterClosurePredicates(predicates, closureUris);
	// Other predicates are untouched
	assert.ok(SOME_PRED in predicates, 'other predicates should not be touched');
	assert.equal(predicates[SOME_PRED].values.length, 1);
});

test('filterClosurePredicates: no-op for single-URI closure', () => {
	const closureUris = ['https://a.example/'];
	const predicates = {
		[OWL_SAME_AS]: {
			label: 'same as',
			type: 'uri',
			values: [{ uri: 'http://dbpedia.org/resource/A', label: 'A (DBpedia)' }],
		},
	};
	filterClosurePredicates(predicates, closureUris);
	// Single-URI closure: no filtering of any values (nothing is a "closure member")
	assert.ok(OWL_SAME_AS in predicates, 'single-URI closure: predicate kept as-is');
	assert.equal(predicates[OWL_SAME_AS].values.length, 1);
});

test('filterClosurePredicates: keeps literal values in owl:sameAs unchanged', () => {
	// literals have no .uri, so they should never be filtered
	const closureUris = ['https://a.example/', 'https://b.example/'];
	const predicates = {
		[OWL_SAME_AS]: {
			label: 'same as',
			type: 'literal',
			values: [{ label: 'some literal' }],
		},
	};
	filterClosurePredicates(predicates, closureUris);
	assert.ok(OWL_SAME_AS in predicates, 'literal values should not be filtered');
});
