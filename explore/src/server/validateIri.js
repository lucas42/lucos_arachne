/**
 * Validate that a user-supplied string is safe to embed in a SPARQL IRI
 * position (i.e. inside `<…>`).
 *
 * A `>` character closes the IRI delimiter early, allowing arbitrary SPARQL
 * to be injected into any query that interpolates the value.  Whitespace is
 * also forbidden in SPARQL IRIs per the grammar.
 *
 * @param {string|undefined} value - The raw query-parameter value to validate.
 * @param {string} paramName - Name of the parameter (used in the error message).
 * @returns {string} The validated value, unchanged.
 * @throws {Error} With `status: 400` if the value is absent or unsafe.
 */
export function validateIri(value, paramName) {
	if (!value || typeof value !== 'string' || value.includes('>') || /\s/.test(value)) {
		const err = new Error(`Invalid IRI in query parameter '${paramName}'`);
		err.status = 400;
		throw err;
	}
	return value;
}
