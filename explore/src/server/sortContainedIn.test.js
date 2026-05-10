import { test } from 'node:test';
import assert from 'node:assert/strict';
import { sortContainedIn } from './sortContainedIn.js';

// ─── helpers ─────────────────────────────────────────────────────────────────

function place(id, label) {
	return { uri: `https://example.org/place/${id}/`, label };
}

function pair(child, parent) {
	return {
		child: `https://example.org/place/${child}/`,
		parent: `https://example.org/place/${parent}/`,
	};
}

// ─── trivial cases ───────────────────────────────────────────────────────────

test('empty array returns empty array', () => {
	assert.deepEqual(sortContainedIn([], []), []);
});

test('single value returned unchanged', () => {
	const values = [place(1, 'Europe')];
	const result = sortContainedIn(values, []);
	assert.equal(result.length, 1);
	assert.equal(result[0].label, 'Europe');
	assert.equal(result[0].also, undefined);
});

// ─── linear chain (no forks) ─────────────────────────────────────────────────

test('linear chain: most general first, most specific last', () => {
	// Europe → UK → Scotland (transitive closure gives all pairs)
	const values = [place(1, 'Europe'), place(2, 'UK'), place(3, 'Scotland')];
	const chainPairs = [
		// Scotland containedIn both UK and Europe
		pair(3, 2), pair(3, 1),
		// UK containedIn Europe
		pair(2, 1),
	];
	const result = sortContainedIn(values, chainPairs);
	assert.deepEqual(result.map(v => v.label), ['Europe', 'UK', 'Scotland']);
	assert.equal(result[0].also, undefined);
	assert.equal(result[1].also, undefined);
	assert.equal(result[2].also, undefined);
});

test('linear chain of four: correct depth order', () => {
	const values = [
		place(1, 'Observable Universe'),
		place(2, 'Milky Way'),
		place(3, 'Solar System'),
		place(4, 'Earth'),
	];
	const chainPairs = [
		pair(4, 3), pair(4, 2), pair(4, 1),
		pair(3, 2), pair(3, 1),
		pair(2, 1),
	];
	const result = sortContainedIn(values, chainPairs);
	assert.deepEqual(result.map(v => v.label), [
		'Observable Universe', 'Milky Way', 'Solar System', 'Earth',
	]);
});

// ─── fork at deepest level ───────────────────────────────────────────────────

test('fork: two equal-depth parents, alphabetically-first is primary', () => {
	// Subject has two direct parents: Edinburgh and City of Edinburgh
	// Both have the same ancestors: Scotland, UK, Europe
	const values = [
		place(1, 'Europe'),
		place(2, 'UK'),
		place(3, 'Scotland'),
		place(4, 'Edinburgh'),
		place(5, 'City of Edinburgh'),
	];
	const chainPairs = [
		// Edinburgh and City of Edinburgh both containedIn Scotland, UK, Europe
		pair(4, 3), pair(4, 2), pair(4, 1),
		pair(5, 3), pair(5, 2), pair(5, 1),
		// Scotland containedIn UK and Europe
		pair(3, 2), pair(3, 1),
		// UK containedIn Europe
		pair(2, 1),
	];
	const result = sortContainedIn(values, chainPairs);

	// Depths: Europe=0, UK=1, Scotland=2, Edinburgh=3, City of Edinburgh=3
	// At depth 3: "City of Edinburgh" < "Edinburgh" alphabetically → City of Edinburgh is primary
	assert.deepEqual(result.map(v => v.label), ['Europe', 'UK', 'Scotland', 'City of Edinburgh']);
	assert.equal(result[3].also?.length, 1);
	assert.equal(result[3].also[0].label, 'Edinburgh');
});

test('fork: primary preserves its uri', () => {
	const values = [place(1, 'Country A'), place(2, 'Country B')];
	const chainPairs = []; // Both at depth 0, no containedIn between them
	const result = sortContainedIn(values, chainPairs);
	assert.equal(result.length, 1);
	assert.equal(result[0].label, 'Country A'); // alphabetically first
	assert.equal(result[0].uri, 'https://example.org/place/1/');
	assert.equal(result[0].also?.[0].label, 'Country B');
});

test('fork with three siblings: one primary, two in also', () => {
	const values = [place(1, 'Zeta'), place(2, 'Alpha'), place(3, 'Mu')];
	const chainPairs = []; // All at depth 0
	const result = sortContainedIn(values, chainPairs);
	assert.equal(result.length, 1);
	assert.equal(result[0].label, 'Alpha'); // alphabetically first
	assert.deepEqual(result[0].also?.map(v => v.label), ['Mu', 'Zeta']); // remaining in alpha order
});

// ─── fork mid-chain ───────────────────────────────────────────────────────────

test('fork at non-deepest level: annotation appears at that level', () => {
	// Two parents at depth 1 (Region A and Region B), both contained in Continent
	// Subject's direct parent is City, which is inside both regions (hypothetically)
	const values = [
		place(1, 'Continent'),
		place(2, 'Region A'),
		place(3, 'Region B'),
		place(4, 'City'),
	];
	const chainPairs = [
		// City containedIn Region A, Region B, Continent
		pair(4, 2), pair(4, 3), pair(4, 1),
		// Region A containedIn Continent
		pair(2, 1),
		// Region B containedIn Continent
		pair(3, 1),
	];
	const result = sortContainedIn(values, chainPairs);
	// Depths: Continent=0, Region A=1, Region B=1, City=3
	// At depth 1: "Region A" < "Region B" → Region A is primary, Region B in also
	assert.equal(result.length, 3); // Continent, Region A (+ also: Region B), City
	assert.equal(result[0].label, 'Continent');
	assert.equal(result[1].label, 'Region A');
	assert.equal(result[1].also?.[0].label, 'Region B');
	assert.equal(result[2].label, 'City');
});

// ─── no pairs in set ──────────────────────────────────────────────────────────

test('pairs outside the value set are ignored', () => {
	const values = [place(1, 'Europe'), place(2, 'Scotland')];
	// Pairs reference place/99 which is not in the values set
	const chainPairs = [
		{ child: 'https://example.org/place/2/', parent: 'https://example.org/place/99/' },
		{ child: 'https://example.org/place/99/', parent: 'https://example.org/place/1/' },
	];
	const result = sortContainedIn(values, chainPairs);
	// Neither Scotland nor Europe has ancestors in the values set → both depth 0 → fork
	// "Europe" < "Scotland" → Europe is primary
	assert.equal(result.length, 1);
	assert.equal(result[0].label, 'Europe');
	assert.equal(result[0].also?.[0].label, 'Scotland');
});

test('values with no pairs: all at depth 0, sorted alphabetically, first is primary', () => {
	const values = [place(1, 'Zebra Land'), place(2, 'Apple Land')];
	const result = sortContainedIn(values, []);
	assert.equal(result.length, 1);
	assert.equal(result[0].label, 'Apple Land');
	assert.equal(result[0].also?.[0].label, 'Zebra Land');
});
