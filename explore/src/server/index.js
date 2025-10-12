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
	const q = req.query.q;
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
