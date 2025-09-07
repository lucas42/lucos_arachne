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
	res.json({
		system: 'lucos_arachne',
		checks: {
		},
		metrics: {
		},
		ci: {
			circle: "gh/lucas42/lucos_arachne",
		},
		network_only: true,
		title: "Arachne",
		show_on_homepage: true,
		icon: "/icon.png",
	});
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
