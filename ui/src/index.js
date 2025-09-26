import express from 'express';
import { middleware as authMiddleware } from './auth.js';
import { createProxyMiddleware } from "http-proxy-middleware";

export class BadRequestError extends Error { }
export class AuthError extends Error { }

const app = express();
app.auth = authMiddleware;
const port = process.env.PORT;

app.set('view engine', 'ejs');
app.use(express.static('./resources', {extensions: ['json']}));

// Avoid authentication for _info, so call before invoking auth middleware
app.get('/_info', catchErrors(async (req, res) => {
	const output ={
		system: 'lucos_arachne',
		checks: {
			/*sparql: {
				techDetail: 'Checks a query can be made to the sparql endpoint'
			}*/
		},
		metrics: {
			/*triples: {
				techDetail: 'Number of triples in default graph of triplestore',
			},*/
		},
		ci: {
			circle: "gh/lucas42/lucos_arachne",
		},
		network_only: true,
		title: "Arachne",
		show_on_homepage: true,
		icon: "/icon.png",
	};
	// Commenting out as it's being extremely flakey and filling up my inbox
	/*try {
		const body = new URLSearchParams();
		body.append("query", "SELECT (COUNT(*) as ?triplecount) \nWHERE { ?s ?p ?o } ");
		const response = await fetch("http://triplestore:3030/arachne/", {
			method: "POST",
			body,
			headers: {
				Authorization: `Basic ${btoa(`lucos_arachne:${process.env.KEY_LUCOS_ARACHNE}`)}`,
			},
			signal: AbortSignal.timeout(900),
		});
		const data = await response.json();
		const triplecount = parseInt(data.results.bindings[0].triplecount.value);
		output.checks.sparql.ok = true;
		output.metrics.triples.value = triplecount;
	} catch (error) {
		output.checks.sparql.ok = false;
		output.checks.sparql.debug = error.message
		delete output.metrics.triples;
	}*/

	res.json(output);
}));

// Do this before auth middleware as the authnentication is done with API keys (which get passed through to the search engine to check)
app.use('/basic-search', catchErrors(async (req, res, next) => {
	if (req.method != 'OPTIONS') return next();
	res.set({
		"Access-Control-Allow-Methods": "GET",
		"Access-Control-Allow-Headers": "Authorization",
		"Access-Control-Allow-Origin": "*",
	});
	res.status(204).send();
}));
app.get('/basic-search', catchErrors(async (req, res) => {
	res.set("Access-Control-Allow-Origin", "*");
	if (!req.query.q) throw new BadRequestError("No `q` query parameter given");
	const auth = req.headers["authorization"];
	if (!auth) throw new BadRequestError("Authorization header not set");
	const [auth_type, auth_val] = auth.split(" ", 2);
	if (auth_type != 'key') throw new AuthError(`Unrecognised authorization type ${auth_type}`);
	if (!auth_val) throw new AuthError("No key found in Authorization header");
	const queryParams = new URLSearchParams({
		q: req.query.q,
		query_by: "pref_label,labels,description,lyrics",
		query_by_weights: "10,8,3,1",
		sort_by: "_text_match:desc,pref_label:asc",
		prioritize_num_matching_fields: false,
		include_fields: "id,pref_label,type"
	});
	if (req.query.page) queryParams.set("page", req.query.page);
	const filters = [];
	if (req.query.types) {
		filters.push(`type:[${req.query.types}]`);
	}
	if (req.query.exclude_types) {
		filters.push(`type:![${req.query.exclude_types}]`);
	}
	if (req.query.ids) {
		filters.push(`id:[${req.query.ids}]`);
	}
	if (filters.length > 0) {
		queryParams.set("filter_by", filters.join(" && "))
	}
	const response = await fetch("http://search:8108/collections/items/documents/search?"+queryParams.toString(), {
		headers: { 'X-TYPESENSE-API-KEY': auth_val },
		signal: AbortSignal.timeout(900),
	});
	const data = await response.json();
	if (!response.ok) {
		if (response.status == 401 || response.status == 403) throw new AuthError("Invalid API key given");
		throw new Error(`Recieved ${response.status} error from backend: ${data["message"]}`);
	}
	res.json(data);
}));

app.use((req, res, next) => app.auth(req, res, next));

app.get('/', catchErrors(async (req, res) => {
	res.render('index', {});
}));

app.use(
	"/sparql",
	createProxyMiddleware({
		target: "http://triplestore:3030/arachne/",
		auth: `lucos_arachne:${process.env.KEY_LUCOS_ARACHNE}`,
	})
);

// Error Handler
app.use((error, req, res, next) => {

	// Set the status based on the type of error
	if (error instanceof BadRequestError) {
		res.status(400);
	} else if(error instanceof AuthError) {
		res.status(401);
	} else {
		res.status(500);
		console.error(error.stack);
	}

	res.json({errorMessage: error.message});
});

app.listen(port, () => {
	console.log(`UI listening on port ${port}`)
});

// Wrapper for controller async functions which catches errors and sends them on to express' error handling
function catchErrors(controllerFunc) {
	return ((req, res, next) => {
		controllerFunc(req, res, next).catch(error => next(error));
	});
}
