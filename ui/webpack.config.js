const path = require('path');

module.exports = {
  entry: './src/index.js',
  output: {
    filename: 'main.js',
    path: path.resolve(__dirname, 'dist'),
    publicPath: "/"
  },
  module: {
     rules: [
        {
          test: /\.m?js$/,
          exclude: /(node_modules)/,
          use: {
            loader: 'babel-loader',
            options: {
              presets: ['@babel/preset-env']
            }
          }
        }
      ]
  },
  devtool: 'source-map',
  devServer: {
    compress: true,
    port: 9004,
    proxy: {
    '/drypipe': {
        target: 'http://127.0.0.1:5000',
        secure: false,
        ws: true
      },
     '/socket.io': {
        target: 'http://127.0.0.1:5000',
        secure: false,
        ws: true
      }
    }
  }
};