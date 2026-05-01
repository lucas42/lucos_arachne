import express from 'express';
import net from 'net';
import rateLimit from 'express-rate-limit';
import { middleware as authMiddleware } from './auth.js';
import { processBindings } from './processBindings.js';

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
	const WARN_MS = 400;
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
			signal: AbortSignal.timeout(450),
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
	res.render('index', {
		sparql_auth: Buffer.from(`lucos_arachne:${process.env.KEY_LUCOS_ARACHNE}`).toString('base64'),
	});
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
		include_fields: "id,pref_label,type,labels",
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
	const data = await response.json();
	if (!response.ok) {
		throw new Error(`Recieved ${response.status} error from search endpoint: ${data["message"]}`);
	}
	res.render('search', data);
}));
app.get('/item', catchErrors(async (req, res) => {
	const uri = req.query.uri;
	if (!uri) {
		res.redirect(302, '/explore');
		return;
	}
	const requestBody = new URLSearchParams({
		query: `
		PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
		PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
		SELECT ?predicate ?predicateLabel ?predicateLabelRdfs ?object ?objectLabel ?objectLabelRdfs
		WHERE {
			BIND(<${uri}> AS ?subject)
			?subject ?predicate ?object .
			OPTIONAL { ?predicate skos:prefLabel ?predicateLabel }.
			OPTIONAL { ?predicate rdfs:label ?predicateLabelRdfs }.
			OPTIONAL { ?object skos:prefLabel ?objectLabel . }
			OPTIONAL { ?object rdfs:label ?objectLabelRdfs . }
		}
		ORDER BY COALESCE(?predicateLabel, ?predicateLabelRdfs)
		LIMIT 1000
		`,
	})
	const response = await fetch("http://triplestore:3030/arachne/", {
		method: 'POST',
		headers: {
			"authorization": `basic ${Buffer.from(`lucos_arachne:${process.env.KEY_LUCOS_ARACHNE}`).toString('base64')}`,
			"accept": "application/sparql-results+json,*/*;q=0.9",
			"content-type": "application/x-www-form-urlencoded",
		},
		signal: AbortSignal.timeout(2900),
		"body": requestBody.toString(),
	});
	const data = await response.json();
	if (!response.ok) {
		throw new Error(`Recieved ${response.status} error from sparql endpoint: ${data["message"]}`);
	}
	const { prefLabel, types, predicates, wikipediaLink } = processBindings(data.results.bindings);
	res.render('item', {
		uri,
		types,
		prefLabel,
		predicates,
		wikipediaLink,
	});
}));

// Error Handler
app.use((error, req, res, next) => {
	res.status(500);
	console.error(error.stack);
	res.json({errorMessage: error.message});
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
