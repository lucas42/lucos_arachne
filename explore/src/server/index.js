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
