import express from 'express';
import { middleware as authMiddleware } from './auth.js';
import { createProxyMiddleware } from "http-proxy-middleware";

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

app.listen(port, () => {
	console.log(`UI listening on port ${port}`)
});

// Wrapper for controller async functions which catches errors and sends them on to express' error handling
function catchErrors(controllerFunc) {
	return ((req, res, next) => {
		controllerFunc(req, res).catch(error => next(error));
	});
}
