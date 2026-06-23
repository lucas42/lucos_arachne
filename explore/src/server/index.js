import express from 'express';
import net from 'net';
import rateLimit from 'express-rate-limit';
import { middleware as authMiddleware } from './auth.js';
import { processBindings, processPhaseACounts, processPhaseCBindings, bestLabelAcrossRows } from './processBindings.js';
import { computeDisplayLabels } from './disambiguate.js';
import { formatYearRange } from './formatYearRange.js';
import { sortContainedIn } from './sortContainedIn.js';
import { validateIri } from './validateIri.js';
import { findPrimaryUri, shouldRedirectToPrimary, filterClosurePredicates, buildClosureLinks } from './sameAsHelpers.js';

const app = express();
app.auth = authMiddleware;

app.set('view engine', 'ejs');
app.use(express.static('./resources', {extensions: ['json']}));

// /_info is unauthenticated and must be registered before the auth middleware.
app.get('/_info', catchErrors(async (req, res) => {
	const search = checkSearch();
	const ingestor = checkIngestor();
	const triplestore = checkTriplestore();
	const sparqlLatency = checkSparqlLatency();
	const [searchResult, ingestorResult, triplestoreResult, sparqlLatencyResult] = await Promise.all([search, ingestor, triplestore, sparqlLatency]);
	const metrics = {};
	if (sparqlLatencyResult.latencyMs !== null) {
		metrics['sparql-query-latency-ms'] = {
			value: sparqlLatencyResult.latencyMs,
			techDetail: 'Wall-clock time in ms for a SELECT COUNT(*) query against the arachne SPARQL endpoint',
		};
	}
	res.json({
		system: 'lucos_arachne',
		checks: {
			search: searchResult,
			ingestor: ingestorResult,
			triplestore: triplestoreResult,
			'sparql-latency': sparqlLatencyResult.check,
		},
		metrics,
		ci: { circle: 'gh/lucas42/lucos_arachne' },
		network_only: true,
		title: 'Arachne',
		show_on_homepage: true,
		icon: '/icon.png',
	});
}));

async function checkSearch() {
	const techDetail = 'GET /collections/items to confirm Typesense is up and the items collection exists';
	const failThreshold = 3;
	try {
		const response = await fetch('http://search:8108/collections/items', {
			headers: { 'X-TYPESENSE-API-KEY': process.env.KEY_LUCOS_ARACHNE },
			signal: AbortSignal.timeout(450),
		});
		if (!response.ok) return { ok: false, techDetail, failThreshold, debug: `HTTP ${response.status}` };
		return { ok: true, techDetail, failThreshold };
	} catch (err) {
		return { ok: false, techDetail, failThreshold, debug: err.message };
	}
}

function checkIngestor() {
	const techDetail = 'TCP connect to ingestor:8099 to confirm the process is running';
	const failThreshold = 3;
	return new Promise((resolve) => {
		const timeout = setTimeout(() => {
			socket.destroy();
			resolve({ ok: false, techDetail, failThreshold, debug: 'timeout' });
		}, 450);
		const socket = net.connect(8099, 'ingestor', () => {
			clearTimeout(timeout);
			socket.end();
			resolve({ ok: true, techDetail, failThreshold });
		});
		socket.on('error', (err) => {
			clearTimeout(timeout);
			resolve({ ok: false, techDetail, failThreshold, debug: err.message });
		});
	});
}

async function checkTriplestore() {
	const techDetail = 'ASK query against http://triplestore:3030/raw_arachne/sparql to confirm the triplestore is up and accepting queries';
	const failThreshold = 7;
	try {
		const body = new URLSearchParams({ query: 'ASK {}' });
		const response = await fetch('http://triplestore:3030/raw_arachne/sparql', {
			method: 'POST',
			headers: {
				'Content-Type': 'application/x-www-form-urlencoded',
				'Accept': 'application/sparql-results+json',
				'Authorization': `Basic ${Buffer.from(`lucos_arachne:${process.env.KEY_LUCOS_ARACHNE}`).toString('base64')}`,
			},
			body: body.toString(),
			signal: AbortSignal.timeout(450),
		});
		if (!response.ok) return { ok: false, techDetail, failThreshold, debug: `HTTP ${response.status}` };
		return { ok: true, techDetail, failThreshold };
	} catch (err) {
		return { ok: false, techDetail, failThreshold, debug: err.message };
	}
}


async function checkSparqlLatency() {
	const techDetail = 'SELECT COUNT(*) query against the arachne endpoint to measure SPARQL query latency';
	const failThreshold = 3;
	const WARN_MS = 1000;
	const start = Date.now();
	try {
		const body = new URLSearchParams({ query: 'SELECT (COUNT(*) AS ?n) WHERE { ?s a ?t }' });
		const response = await fetch('http://triplestore:3030/arachne/sparql', {
			method: 'POST',
			headers: {
				'Content-Type': 'application/x-www-form-urlencoded',
				'Accept': 'application/sparql-results+json',
				'Authorization': `Basic ${Buffer.from(`lucos_arachne:${process.env.KEY_LUCOS_ARACHNE}`).toString('base64')}`,
			},
			body: body.toString(),
			signal: AbortSignal.timeout(1100),
		});
		const latencyMs = Date.now() - start;
		if (!response.ok) return { check: { ok: false, techDetail, failThreshold, debug: `HTTP ${response.status}` }, latencyMs: null };
		const ok = latencyMs < WARN_MS;
		return {
			check: { ok, techDetail, failThreshold, ...(ok ? {} : { debug: `${latencyMs}ms` }) },
			latencyMs,
		};
	} catch (err) {
		return { check: { ok: false, techDetail, failThreshold, debug: err.message }, latencyMs: null };
	}
}


app.set('trust proxy', 1);
app.use(rateLimit({ windowMs: 15 * 60 * 1000, limit: 100 }));
app.use((req, res, next) => app.auth(req, res, next));

app.get('/', catchErrors(async (req, res) => {
	res.render('index');
}));
app.get('/sparql-explorer', catchErrors(async (req, res) => {
	res.render('sparql', {
		sparql_auth: Buffer.from(`lucos_arachne:${process.env.KEY_LUCOS_ARACHNE}`).toString('base64'),
	});
}));
app.get('/timeline', catchErrors(async (req, res) => {
	const bindings = await sparqlFetch(`
		PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
		PREFIX eolas: <https://eolas.l42.eu/ontology/>
		PREFIX time: <http://www.w3.org/2006/time#>
		SELECT ?event ?label ?startYearVal ?endYearVal WHERE {
			?event a eolas:HistoricalEvent .
			?event skos:prefLabel ?label .
			OPTIONAL {
				?event eolas:startYear ?startYearBn .
				?startYearBn time:year ?startYearVal .
			}
			OPTIONAL {
				?event eolas:endYear ?endYearBn .
				?endYearBn time:year ?endYearVal .
			}
		}
		ORDER BY ASC(?startYearVal) ASC(?label)
	`);

	// De-duplicate by event URI (OPTIONAL joins can produce multiple rows per event)
	const seen = new Set();
	const dated = [];
	const undated = [];

	for (const row of bindings) {
		const uri = row.event.value;
		if (seen.has(uri)) continue;
		seen.add(uri);

		const label = row.label.value;
		const startYear = row.startYearVal ? parseInt(row.startYearVal.value, 10) : null;
		const endYear = row.endYearVal ? parseInt(row.endYearVal.value, 10) : null;

		if (startYear !== null) {
			const resolvedEndYear = endYear ?? startYear;
			dated.push({
				uri,
				label,
				startYear,
				endYear: resolvedEndYear,
				dateLabel: formatYearRange(startYear, resolvedEndYear),
			});
		} else {
			undated.push({ uri, label });
		}
	}

	// Compute duration bar widths relative to the longest event
	if (dated.length > 0) {
		const maxDuration = Math.max(...dated.map(e => e.endYear - e.startYear + 1));
		for (const e of dated) {
			e.durationPct = Math.max(1, Math.round((e.endYear - e.startYear + 1) / maxDuration * 100));
		}
	}

	res.render('timeline', { dated, undated });
}));
app.get('/search', catchErrors(async (req, res) => {
	if (!req.query.q) {
		res.redirect(302, '/explore');
		return;
	}
	const searchParams = new URLSearchParams({
		q: req.query.q,
		query_by: "pref_label,labels,description,lyrics",
		query_by_weights: "10,8,3,1",
		include_fields: "id,pref_label,type,labels,contained_in,artist,category",
		sort_by: "_text_match:desc,pref_label:asc",
		prioritize_num_matching_fields: false,
		enable_highlight_v1: false,
		highlight_start_tag: '<span class="highlight">',
		highlight_end_tag: '</span>',
		page: req.query.page || 1,
		per_page: 30,
	});
	const response = await fetch("http://web/search?"+searchParams.toString(), {
		headers: { 'X-TYPESENSE-API-KEY': process.env.KEY_LUCOS_ARACHNE },
		signal: AbortSignal.timeout(900),
	});
	if (!response.ok) {
		throw new Error(`Received ${response.status} error from search endpoint: ${response.statusText}`);
	}
	const data = await response.json();
	// Apply disambiguation: compute display labels only where label collisions exist.
	data.hits = computeDisplayLabels(data.hits);
	res.render('search', data);
}));
// High-fan-out threshold: predicates with more than this many objects are paginated.
const HIGH_FAN_OUT_THRESHOLD = 50;

// URI prefix assigned to all skolemised blank nodes at ingest time.
const SKOLEM_PREFIX = 'urn:lucos:skolem:';

// Shared SPARQL fetch helper for the item page.
async function sparqlFetch(query) {
	const requestBody = new URLSearchParams({ query });
	const response = await fetch("http://triplestore:3030/arachne/", {
		method: 'POST',
		headers: {
			"authorization": `basic ${Buffer.from(`lucos_arachne:${process.env.KEY_LUCOS_ARACHNE}`).toString('base64')}`,
			"accept": "application/sparql-results+json,*/*;q=0.9",
			"content-type": "application/x-www-form-urlencoded",
		},
		signal: AbortSignal.timeout(2900),
		body: requestBody.toString(),
	});
	if (!response.ok) {
		throw new Error(`Received ${response.status} error from sparql endpoint: ${response.statusText}`);
	}
	const data = await response.json();
	return data.results.bindings;
}

// Expand any predicate values that are urn:lucos:skolem: URIs (i.e. blank nodes
// assigned a Skolem URI at ingest time) into inline property data.  For each
// unique skolem URI found in the predicates object, a SPARQL query fetches that
// node's own predicate/object pairs, and the raw-URI value is replaced with an
// { inlinePredicates: {...} } object for inline rendering.
//
// Values whose skolem node has no labelled predicates are removed entirely.
// Predicates that end up with no values are also removed.
// Mutates the predicates object in place.
async function expandSkolemInline(predicates) {
	// Collect all unique skolem URIs across every predicate's value list.
	const skolemUris = new Set();
	for (const pred of Object.values(predicates)) {
		for (const value of pred.values) {
			if (value.uri && value.uri.startsWith(SKOLEM_PREFIX)) {
				skolemUris.add(value.uri);
			}
		}
	}
	if (skolemUris.size === 0) return;

	// Fetch properties for each skolem URI in parallel.
	const skolemData = new Map();
	await Promise.all([...skolemUris].map(async (skolemUri) => {
		const bindings = await sparqlFetch(`
			PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
			PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
			SELECT ?predicate ?predicateLabel ?predicateLabelRdfs ?object ?objectLabel ?objectLabelRdfs WHERE {
				BIND(<${skolemUri}> AS ?subject)
				?subject ?predicate ?object .
				OPTIONAL { ?predicate skos:prefLabel ?predicateLabel }
				OPTIONAL { ?predicate rdfs:label ?predicateLabelRdfs }
				OPTIONAL { ?object skos:prefLabel ?objectLabel }
				OPTIONAL { ?object rdfs:label ?objectLabelRdfs }
			}
		`);
		const { predicates: inlinePredicates } = processBindings(bindings);
		skolemData.set(skolemUri, inlinePredicates);
	}));

	// Replace skolem values with their expanded inline data; remove those with
	// nothing to show, and drop predicates whose value list becomes empty.
	for (const [predKey, pred] of Object.entries(predicates)) {
		pred.values = pred.values.flatMap(value => {
			if (!value.uri || !value.uri.startsWith(SKOLEM_PREFIX)) return [value];
			const inlinePredicates = skolemData.get(value.uri) || {};
			if (Object.keys(inlinePredicates).length === 0) return []; // nothing to show
			return [{ inlinePredicates }];
		});
		if (pred.values.length === 0) delete predicates[predKey];
	}
}

// Walk the owl:sameAs closure for a given URI.
// Returns an array of all URI strings reachable via owl:sameAs* (including the
// input URI itself).  Since #568 materialises owl:sameAs as symmetric, a
// one-direction property-path traversal reaches all closure members.
async function getSameAsClosure(uri) {
	const bindings = await sparqlFetch(`
		PREFIX owl: <http://www.w3.org/2002/07/owl#>
		SELECT DISTINCT ?member WHERE {
			<${uri}> owl:sameAs* ?member .
		}
	`);
	return bindings.map(b => b.member.value);
}

// Fetch preferredIdentifier edges for a set of closure URIs.
// Returns a Map<source, target> limited to edges where both ends are in the closure.
async function getPrefIdPairs(closureUris) {
	if (closureUris.length <= 1) return new Map();
	const valuesClause = closureUris.map(u => `<${u}>`).join(' ');
	const bindings = await sparqlFetch(`
		PREFIX eolas: <https://eolas.l42.eu/ontology/>
		SELECT ?subject ?target WHERE {
			VALUES ?subject { ${valuesClause} }
			VALUES ?target { ${valuesClause} }
			?subject eolas:preferredIdentifier ?target .
		}
	`);
	return new Map(bindings.map(b => [b.subject.value, b.target.value]));
}

// Fetch the skos:prefLabel for the primary URI, or null if none exists.
async function getPrimaryPrefLabel(primaryUri) {
	const bindings = await sparqlFetch(`
		PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
		SELECT ?label WHERE {
			<${primaryUri}> skos:prefLabel ?label .
		}
		LIMIT 1
	`);
	return bindings.length > 0 ? bindings[0].label.value : null;
}

app.get('/item', catchErrors(async (req, res) => {
	const uri = req.query.uri;
	if (!uri) {
		res.redirect(302, '/explore');
		return;
	}
	validateIri(uri, 'uri');

	// Walk the owl:sameAs closure and determine the canonical (primary) URI.
	// Runs concurrently: closure walk first, then preferredIdentifier pairs once
	// we know the full closure.
	const closureUris = await getSameAsClosure(uri);
	const prefIdPairs = await getPrefIdPairs(closureUris);
	const primaryUri = findPrimaryUri(closureUris, prefIdPairs);

	// Redirect secondary URIs to the canonical primary.
	// This only fires when there ARE preferredIdentifier edges in the closure — if
	// no edges exist there is no deterministic canonical URL and no redirect.
	if (shouldRedirectToPrimary(uri, primaryUri, prefIdPairs, closureUris)) {
		res.redirect(302, `/item?uri=${encodeURIComponent(primaryUri)}`);
		return;
	}

	// Build the VALUES clause used in all SPARQL queries below.
	// Single-URI closures produce VALUES ?subject { <uri> } which is equivalent
	// to the previous BIND(<uri> AS ?subject) — no behaviour change for items
	// that have no owl:sameAs links.
	const closureValuesClause = closureUris.map(u => `<${u}>`).join(' ');

	// Fetch the primary URI's prefLabel separately so we always use the canonical
	// label as the page title even when merging data from multiple closure members.
	const primaryPrefLabelPromise = getPrimaryPrefLabel(primaryUri);

	// Phase A: per-predicate object counts and labels (one cheap GROUP BY query).
	const phaseABindings = await sparqlFetch(`
		PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
		PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
		SELECT ?p ?pLabel ?pLabelRdfs (COUNT(?o) AS ?count) WHERE {
			VALUES ?subject { ${closureValuesClause} }
			?subject ?p ?o .
			OPTIONAL { ?p skos:prefLabel ?pLabel }
			OPTIONAL { ?p rdfs:label ?pLabelRdfs }
		}
		GROUP BY ?p ?pLabel ?pLabelRdfs
	`);
	const predicateCounts = processPhaseACounts(phaseABindings);

	// Separate predicates into small (≤ threshold) and large (> threshold).
	const smallPredicates = [];
	const largePredicates = [];
	for (const [predUri, { count }] of predicateCounts) {
		if (count <= HIGH_FAN_OUT_THRESHOLD) {
			smallPredicates.push(predUri);
		} else {
			largePredicates.push(predUri);
		}
	}

	// Phase B: fetch objects for all small predicates in one bounded query.
	// Uses the same variable names as the original query so processBindings works unchanged.
	let phaseBBindings = [];
	if (smallPredicates.length > 0) {
		const valuesClause = smallPredicates.map(p => `<${p}>`).join(' ');
		phaseBBindings = await sparqlFetch(`
			PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
			PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
			SELECT ?predicate ?predicateLabel ?predicateLabelRdfs ?object ?objectLabel ?objectLabelRdfs WHERE {
				VALUES ?predicate { ${valuesClause} }
				VALUES ?subject { ${closureValuesClause} }
				?subject ?predicate ?object .
				OPTIONAL { ?predicate skos:prefLabel ?predicateLabel }
				OPTIONAL { ?predicate rdfs:label ?predicateLabelRdfs }
				OPTIONAL { ?object skos:prefLabel ?objectLabel }
				OPTIONAL { ?object rdfs:label ?objectLabelRdfs }
			}
		`);
	}

	const {
		prefLabel: fallbackPrefLabel,
		types,
		predicates,
		wikipediaLink,
	} = processBindings(phaseBBindings);

	// Prefer the primary URI's own skos:prefLabel as the page title; fall back
	// to whatever processBindings extracted (which may come from any closure member).
	const prefLabel = (await primaryPrefLabelPromise) ?? fallbackPrefLabel;

	// Strip owl:sameAs and preferredIdentifier values that are only closure members
	// — these are merge-plumbing, not facts about the entity worth showing.
	filterClosurePredicates(predicates, closureUris);

	// Annotate every small predicate with its total count (not truncated).
	for (const predUri of Object.keys(predicates)) {
		const cd = predicateCounts.get(predUri);
		predicates[predUri].count = cd ? cd.count : null;
		predicates[predUri].truncated = false;
	}

	// Phase C: first N objects for each large predicate, fetched in parallel.
	// ORDER BY label then URI for deterministic pagination.
	if (largePredicates.length > 0) {
		await Promise.all(largePredicates.map(async (predUri) => {
			const phaseCBindings = await sparqlFetch(`
				PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
				PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
				SELECT ?object ?objectLabel ?objectLabelRdfs WHERE {
					VALUES ?subject { ${closureValuesClause} }
					?subject <${predUri}> ?object .
					OPTIONAL { ?object skos:prefLabel ?objectLabel }
					OPTIONAL { ?object rdfs:label ?objectLabelRdfs }
				}
				ORDER BY ASC(COALESCE(?objectLabel, ?objectLabelRdfs, ?object)) ASC(?object)
				LIMIT ${HIGH_FAN_OUT_THRESHOLD}
			`);

			const { count, label } = predicateCounts.get(predUri);
			const values = processPhaseCBindings(phaseCBindings);
			// Detect the object type from the first binding (used for consistent rendering).
			const firstObj = phaseCBindings.find(b => b.object.type !== 'bnode');
			const type = firstObj ? firstObj.object.type : 'uri';

			predicates[predUri] = {
				label,
				type,
				count,
				truncated: true,
				values,
			};
		}));
	}

	// Expand any skolem URI values inline before Phase D so containedIn filtering
	// correctly ignores blank-node values (which wouldn't be place URIs anyway).
	await expandSkolemInline(predicates);

	// Phase D: topological sort for containedIn places.
	// The inferred triplestore materialises all transitive containedIn pairs, so
	// a VALUES-based query over the containedIn value set gives us the full chain
	// structure we need to sort by depth (most-general first) and detect forks.
	const CONTAINED_IN_PRED = 'https://eolas.l42.eu/ontology/containedIn';
	const containedInPred = predicates[CONTAINED_IN_PRED];
	if (containedInPred) {
		const uriValues = containedInPred.values.filter(v => v.uri);
		if (uriValues.length > 1) {
			const valuesClause = uriValues.map(v => `<${v.uri}>`).join(' ');
			const chainBindings = await sparqlFetch(`
				PREFIX eolas: <https://eolas.l42.eu/ontology/>
				SELECT ?child ?parent WHERE {
					VALUES ?child { ${valuesClause} }
					VALUES ?parent { ${valuesClause} }
					?child eolas:containedIn ?parent .
				}
			`);
			const chainPairs = chainBindings.map(row => ({
				child: row.child.value,
				parent: row.parent.value,
			}));
			containedInPred.values = sortContainedIn(uriValues, chainPairs);
		}
	}

	// Fetch the category for this item (via its type's eolas:hasCategory relation).
	const categoryBindings = await sparqlFetch(`
		PREFIX eolas: <https://eolas.l42.eu/ontology/>
		PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
		SELECT ?categoryLabel WHERE {
			VALUES ?subject { ${closureValuesClause} }
			?subject a ?type .
			?type eolas:hasCategory ?category .
			?category skos:prefLabel ?categoryLabel .
			FILTER(LANG(?categoryLabel) = 'en')
		}
		LIMIT 1
	`);
	const category = categoryBindings.length > 0 ? categoryBindings[0].categoryLabel.value : null;

	const closureLinks = buildClosureLinks(primaryUri, closureUris);

	res.render('item', {
		uri,
		types,
		prefLabel,
		predicates,
		wikipediaLink,
		category,
		closureLinks,
	});
}));

app.get('/predicate-objects', catchErrors(async (req, res) => {
	const uri = req.query.uri;
	const predicate = req.query.predicate;
	if (!uri || !predicate) {
		res.redirect(302, '/explore');
		return;
	}
	validateIri(uri, 'uri');
	validateIri(predicate, 'predicate');

	// Walk the owl:sameAs closure and find the primary URI, exactly as the /item
	// handler does, so that objects contributed by secondary closure members are
	// included in both the count and the paged results.
	const closureUris = await getSameAsClosure(uri);
	const prefIdPairs = await getPrefIdPairs(closureUris);
	const primaryUri = findPrimaryUri(closureUris, prefIdPairs);
	const closureValuesClause = closureUris.map(u => `<${u}>`).join(' ');

	const PAGE_SIZE = 50;
	const page = Math.max(1, parseInt(req.query.page, 10) || 1);
	const offset = (page - 1) * PAGE_SIZE;

	// Run three queries in parallel:
	//   meta:  predicate label and total object count across the full closure
	//   page:  paged objects from the full closure
	//   label: primary URI's preferred label for the page heading
	// Subject labels are fetched separately via getPrimaryPrefLabel rather than
	// being included in the meta GROUP BY — mixing them in would split the count
	// across label-groups when closure members carry different labels.
	const [metaBindings, pageBindings, primaryPrefLabel] = await Promise.all([
		sparqlFetch(`
			PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
			PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
			SELECT ?predicateLabel ?predicateLabelRdfs (COUNT(?o) AS ?count) WHERE {
				VALUES ?subject { ${closureValuesClause} }
				BIND(<${predicate}> AS ?pred)
				OPTIONAL { ?pred skos:prefLabel ?predicateLabel }
				OPTIONAL { ?pred rdfs:label ?predicateLabelRdfs }
				?subject ?pred ?o .
			}
			GROUP BY ?predicateLabel ?predicateLabelRdfs
		`),
		sparqlFetch(`
			PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
			PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
			SELECT ?object ?objectLabel ?objectLabelRdfs WHERE {
				VALUES ?subject { ${closureValuesClause} }
				?subject <${predicate}> ?object .
				OPTIONAL { ?object skos:prefLabel ?objectLabel }
				OPTIONAL { ?object rdfs:label ?objectLabelRdfs }
			}
			ORDER BY ASC(COALESCE(?objectLabel, ?objectLabelRdfs, ?object)) ASC(?object)
			LIMIT ${PAGE_SIZE} OFFSET ${offset}
		`),
		getPrimaryPrefLabel(primaryUri),
	]);

	const subjectLabel = primaryPrefLabel || uri;
	const predicateLabel = bestLabelAcrossRows(metaBindings, 'predicateLabel', 'predicateLabelRdfs') || predicate;
	const totalCount = metaBindings.length > 0
		? Math.max(...metaBindings.map(r => parseInt(r.count.value, 10)))
		: 0;

	const values = processPhaseCBindings(pageBindings);
	const totalPages = Math.ceil(totalCount / PAGE_SIZE);

	res.render('predicate-objects', {
		uri,
		subjectLabel,
		predicate,
		predicateLabel,
		totalCount,
		values,
		page,
		totalPages,
		pageSize: PAGE_SIZE,
	});
}));

// Error Handler
app.use((error, req, res, next) => {
	res.status(error.status || 500);
	console.error(error.stack);
	if (req.accepts('html')) {
		res.render('error', {
				title: 'Something went wrong',
				message: error.message,
				hint: 'Try going back and searching again. If the problem persists, search may be temporarily unavailable.',
			});
	} else {
		res.json({errorMessage: error.message});
	}
});

// Wrapper for controller async functions which catches errors and sends them on to express' error handling
function catchErrors(controllerFunc) {
	return ((req, res, next) => {
		controllerFunc(req, res, next).catch(error => next(error));
	});
}

const port = process.env.PORT;
app.listen(port, () => {
	console.log(`Explore UI listening on port ${port}`)
});
