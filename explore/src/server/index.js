import express from 'express';
import { middleware as authMiddleware } from './auth.js';

const app = express();
app.auth = authMiddleware;

app.set('view engine', 'ejs');
app.use(express.static('./resources', {extensions: ['json']}));

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
	const response = await fetch("http://web:8033/search?"+searchParams.toString(), {
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
		SELECT ?predicate ?predicateLabel ?object ?objectLabel
		WHERE {
			BIND(<${uri}> AS ?subject)
			?subject ?predicate ?object .
			OPTIONAL { ?predicate skos:prefLabel ?predicateLabel }.
			OPTIONAL { ?object skos:prefLabel ?objectLabel . }
		}
		ORDER BY ?predicateLabel
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
		signal: AbortSignal.timeout(900),
		"body": requestBody.toString(),
	});
	const data = await response.json();
	if (!response.ok) {
		throw new Error(`Recieved ${response.status} error from sparql endpoint: ${data["message"]}`);
	}
	let prefLabel = null;
	let predicates = {};
	let types = [];
	data.results.bindings.forEach(rel => {
		// Store a single prefLabel to use as title
		if (rel.predicate.value == 'http://www.w3.org/2004/02/skos/core#prefLabel') {
			prefLabel = rel.object.value;
			return;
		}
		// Store a lists of types (limited to only types with a label)
		if (rel.predicate.value == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' && rel.objectLabel) {
			// Only store labels in English or don't have a lang specified
			if (!rel.objectLabel['xml:lang'] || rel.objectLabel['xml:lang'] == 'en') {
				types.push(rel.objectLabel.value);
			}
			return;
		}
		if (!rel.predicateLabel) return;
		if (rel.object.type == 'bnode') return; // Ignore bnodes for now
		if (!(rel.predicate.value in predicates)) predicates[rel.predicate.value] = {
			label: rel.predicateLabel?.value,
			type: rel.object.type,
			values: [],
		};

		let value = null;
		switch (rel.object.type) {
			case 'literal':
				value = {
					label: rel.object.value || 'unknown',
				};
				break;
			case 'uri':
				value = {
					uri: rel.object.value,
					label: rel.objectLabel?.value || rel.object.value || 'unknown',
				};
				break;
			default:
				throw new Error(`Can't render object type ${rel.object.type}`);
		}
		predicates[rel.predicate.value].values.push(value);
	});

	// Sort the values in each predicate by label
	Object.values(predicates).forEach(predicate => {
		predicate.values.sort((a, b) =>
			a.label.replace(/\W/g, '').localeCompare(b.label.replace(/\W/g, '')
		));
	});
	res.render('item', {
		uri,
		types,
		prefLabel,
		predicates,
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
