import { test } from 'node:test';
import assert from 'node:assert/strict';
import { formatYearRange } from './formatYearRange.js';

// ─── point events ────────────────────────────────────────────────────────────

test('point event CE: shows single year with no suffix', () => {
	assert.equal(formatYearRange(1936, 1936), '1936');
});

test('point event BCE: shows absolute year with BCE suffix', () => {
	assert.equal(formatYearRange(-50, -50), '50 BCE');
});

test('point event year 1 CE: shows 1', () => {
	assert.equal(formatYearRange(1, 1), '1');
});

// ─── both CE ─────────────────────────────────────────────────────────────────

test('multi-year CE: shows range with no suffix', () => {
	assert.equal(formatYearRange(1939, 1945), '1939–1945');
});

test('multi-year CE adjacent years', () => {
	assert.equal(formatYearRange(1984, 1985), '1984–1985');
});

// ─── both BCE ────────────────────────────────────────────────────────────────

test('multi-year BCE: shows absolute range with BCE suffix', () => {
	assert.equal(formatYearRange(-167, -141), '167–141 BCE');
});

test('BCE point event', () => {
	assert.equal(formatYearRange(-100, -100), '100 BCE');
});

// ─── BCE → CE crossing ───────────────────────────────────────────────────────

test('BCE to CE crossing: shows both eras explicitly', () => {
	assert.equal(formatYearRange(-27, 476), '27 BCE – 476 CE');
});

test('BCE to CE crossing starting at -1', () => {
	assert.equal(formatYearRange(-1, 1), '1 BCE – 1 CE');
});

// ─── modern events ───────────────────────────────────────────────────────────

test('modern events with same start and end in 21st century', () => {
	assert.equal(formatYearRange(2001, 2001), '2001');
});

test('modern multi-year event', () => {
	assert.equal(formatYearRange(1966, 1998), '1966–1998');
});
