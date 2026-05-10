import { test } from 'node:test';
import assert from 'node:assert/strict';
import { computeDisplayLabels } from './disambiguate.js';

// ─── helpers ─────────────────────────────────────────────────────────────────

function makeHit(pref_label, type, extras = {}) {
	return { document: { pref_label, type, ...extras } };
}

// ─── no-collision cases ───────────────────────────────────────────────────────

test('unique label: displayed as-is without any suffix', () => {
	const hits = [makeHit('London', 'City')];
	const result = computeDisplayLabels(hits);
	assert.equal(result[0].displayLabel, 'London');
});

test('all unique labels: none gets a suffix', () => {
	const hits = [
		makeHit('London', 'City'),
		makeHit('Paris', 'City'),
		makeHit('Berlin', 'City'),
	];
	const result = computeDisplayLabels(hits);
	assert.equal(result[0].displayLabel, 'London');
	assert.equal(result[1].displayLabel, 'Paris');
	assert.equal(result[2].displayLabel, 'Berlin');
});

// ─── rule 1: different types ──────────────────────────────────────────────────

test('rule 1: same label, different types → show type in brackets', () => {
	const hits = [
		makeHit('Springfield', 'City'),
		makeHit('Springfield', 'Historical Event'),
	];
	const result = computeDisplayLabels(hits);
	assert.equal(result[0].displayLabel, 'Springfield (City)');
	assert.equal(result[1].displayLabel, 'Springfield (Historical Event)');
});

test('rule 1: three items with same label, mixed types → all show types', () => {
	const hits = [
		makeHit('Apollo', 'Person'),
		makeHit('Apollo', 'Spacecraft'),
		makeHit('Apollo', 'Music Artist'),
	];
	const result = computeDisplayLabels(hits);
	for (const r of result) {
		assert.ok(r.displayLabel.includes('('), `Expected brackets in "${r.displayLabel}"`);
	}
});

// ─── rule 2: same-type places (contained_in) ──────────────────────────────────

test('rule 2: same type, contained_in present → comma-separated', () => {
	const hits = [
		makeHit('Springfield', 'City', { contained_in: 'Illinois' }),
		makeHit('Springfield', 'City', { contained_in: 'Ohio' }),
	];
	const result = computeDisplayLabels(hits);
	assert.equal(result[0].displayLabel, 'Springfield, Illinois');
	assert.equal(result[1].displayLabel, 'Springfield, Ohio');
});

test('rule 2: unique label with contained_in → no disambiguation suffix', () => {
	const hits = [
		makeHit('London', 'City', { contained_in: 'England' }),
	];
	const result = computeDisplayLabels(hits);
	assert.equal(result[0].displayLabel, 'London');
});

// ─── rule 3 (artist) ─────────────────────────────────────────────────────────

test('rule 3: same type, artist present → bracket format', () => {
	const hits = [
		makeHit('Yesterday', 'Track', { artist: 'The Beatles' }),
		makeHit('Yesterday', 'Track', { artist: 'Matt Monro' }),
	];
	const result = computeDisplayLabels(hits);
	assert.equal(result[0].displayLabel, 'Yesterday (The Beatles)');
	assert.equal(result[1].displayLabel, 'Yesterday (Matt Monro)');
});

// ─── rule 4: fallback ─────────────────────────────────────────────────────────

test('rule 4: same type, no disambiguation data → show type as fallback', () => {
	const hits = [
		makeHit('Bloody Sunday', 'Historical Event'),
		makeHit('Bloody Sunday', 'Historical Event'),
	];
	const result = computeDisplayLabels(hits);
	assert.equal(result[0].displayLabel, 'Bloody Sunday (Historical Event)');
	assert.equal(result[1].displayLabel, 'Bloody Sunday (Historical Event)');
});

// ─── rule priority ────────────────────────────────────────────────────────────

test('rule 1 takes priority over rule 2 when types differ', () => {
	const hits = [
		makeHit('Bath', 'City', { contained_in: 'Somerset' }),
		makeHit('Bath', 'Health Treatment'),
	];
	const result = computeDisplayLabels(hits);
	// Types differ, so rule 1 applies — type in brackets, not comma disambiguation
	assert.equal(result[0].displayLabel, 'Bath (City)');
	assert.equal(result[1].displayLabel, 'Bath (Health Treatment)');
});

// ─── edge cases ───────────────────────────────────────────────────────────────

test('empty input returns empty array', () => {
	const result = computeDisplayLabels([]);
	assert.deepEqual(result, []);
});

test('single item is returned unchanged (with displayLabel added)', () => {
	const hits = [makeHit('Apollo', 'Person')];
	const result = computeDisplayLabels(hits);
	assert.equal(result[0].displayLabel, 'Apollo');
	assert.equal(result[0].document.pref_label, 'Apollo');
});

test('preserves all original hit properties', () => {
	const hits = [
		{ document: { pref_label: 'Paris', type: 'City', id: 'http://example.org/paris', extra: 42 } },
	];
	const result = computeDisplayLabels(hits);
	assert.equal(result[0].document.id, 'http://example.org/paris');
	assert.equal(result[0].document.extra, 42);
});
