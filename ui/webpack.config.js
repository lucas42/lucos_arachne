import { URL } from 'url';
import webpack from 'webpack';
import TerserPlugin from 'terser-webpack-plugin';
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
			// Stop the terser plugin messing with the banner plugin
			minimizer: [new TerserPlugin({
				extractComments: false,
			})],
		},
		devtool: 'source-map',
		mode: 'production',
	};
};