import { URL } from 'url';
import webpack from 'webpack';
import MinimizerPlugin from 'minimizer-webpack-plugin';
import { hashElement } from 'folder-hash';
export default async () => {
	return {
		entry: {
			'client': './client/index.js',
		},
		output: {
			filename: '[name].js',
			path: new URL('./resources/', import.meta.url).pathname,
		},
		module: {
			rules: [
				{
					test: /\.css$/i,
					use: ["style-loader", "css-loader"],
				},
			],
		},
		plugins: [
			// Get the hashes of all the resources to embed in a comment in service worker
			new webpack.BannerPlugin({
				banner: `Resource Hash: ${(await hashElement("./resources")).hash}\nClient JS Hash: ${(await hashElement("./client")).hash}`,
			}),
		],
		optimization: {
			// Stop the minimizer plugin messing with the banner plugin
			// extractComments defaults to true in minimizer-webpack-plugin — keep false to avoid
			// stripping banner comments that the service worker depends on for resource hashes
			minimizer: [new MinimizerPlugin({
				extractComments: false,
			})],
		},
		devtool: 'source-map',
		mode: 'production',
	};
};