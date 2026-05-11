import { validateIri } from './validateIri.js';
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

describe('validateIri', () => {

	// ── Valid inputs ─────────────────────────────────────────────────────────

	it('accepts a well-formed http URI', () => {
		const uri = 'http://example.com/foo';
		assert.equal(validateIri(uri, 'uri'), uri);
	});

	it('accepts a well-formed https URI', () => {
		const uri = 'https://arachne.l42.eu/person/1';
		assert.equal(validateIri(uri, 'uri'), uri);
	});

	it('accepts a URN', () => {
		const urn = 'urn:lucos:skolem:abc123';
		assert.equal(validateIri(urn, 'uri'), urn);
	});

	it('returns the value unchanged when valid', () => {
		const uri = 'https://schema.org/Person';
		assert.equal(validateIri(uri, 'uri'), uri);
	});

	// ── Angle-bracket injection ───────────────────────────────────────────────

	it('rejects a URI containing a closing angle bracket', () => {
		assert.throws(
			() => validateIri('https://example.com/foo>bar', 'uri'),
			(err) => {
				assert.equal(err.status, 400);
				assert.ok(err.message.includes('uri'));
				return true;
			},
		);
	});

	it('rejects the PoC payload from the issue', () => {
		// This is the exact payload from the Proof of Concept in issue #497.
		// If it passes, arbitrary SPARQL would be injected into every query.
		const poc = 'https://example.com>+AS+?x).+?s+?p+?o+.+BIND(<https://example.com';
		assert.throws(
			() => validateIri(poc, 'uri'),
			(err) => err.status === 400,
		);
	});

	// ── Whitespace variants ───────────────────────────────────────────────────

	it('rejects a value containing a space', () => {
		assert.throws(
			() => validateIri('https://example.com/foo bar', 'uri'),
			(err) => err.status === 400,
		);
	});

	it('rejects a value containing a tab', () => {
		assert.throws(
			() => validateIri('https://example.com/foo\tbar', 'uri'),
			(err) => err.status === 400,
		);
	});

	it('rejects a value containing a newline', () => {
		assert.throws(
			() => validateIri('https://example.com/foo\nbar', 'uri'),
			(err) => err.status === 400,
		);
	});

	// ── Missing / empty value ─────────────────────────────────────────────────

	it('rejects undefined', () => {
		assert.throws(
			() => validateIri(undefined, 'uri'),
			(err) => err.status === 400,
		);
	});

	it('rejects an empty string', () => {
		assert.throws(
			() => validateIri('', 'uri'),
			(err) => err.status === 400,
		);
	});

	// ── paramName in error message ────────────────────────────────────────────

	it('includes the paramName in the error message', () => {
		assert.throws(
			() => validateIri('bad>value', 'predicate'),
			(err) => {
				assert.ok(err.message.includes('predicate'));
				return true;
			},
		);
	});

});
