/**
 * Format a year range for display in the historical events timeline.
 * Handles BCE (negative years), CE, point events, and BCE→CE crossings.
 *
 * @param {number} startYear - Start year (negative = BCE, positive = CE)
 * @param {number} endYear   - End year (negative = BCE, positive = CE)
 * @returns {string} - Human-readable date label
 */
export function formatYearRange(startYear, endYear) {
	const startBce = startYear < 0;
	const endBce = endYear < 0;
	const startAbs = Math.abs(startYear);
	const endAbs = Math.abs(endYear);

	if (startYear === endYear) {
		// Point event — show single year
		return startBce ? `${startAbs} BCE` : String(startYear);
	}

	if (startBce && endBce) {
		// Both BCE: e.g. "167–141 BCE"
		return `${startAbs}–${endAbs} BCE`;
	} else if (startBce && !endBce) {
		// BCE → CE crossing: e.g. "27 BCE – 476 CE"
		return `${startAbs} BCE – ${endYear} CE`;
	} else {
		// Both CE: e.g. "1939–1945"
		return `${startYear}–${endYear}`;
	}
}
