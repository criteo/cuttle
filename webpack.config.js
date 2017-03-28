let path = require('path');
let HtmlWebpackPlugin = require('html-webpack-plugin');
let FaviconsWebpackPlugin = require('favicons-webpack-plugin');
let FlowStatusWebpackPlugin = require('flow-status-webpack-plugin');

module.exports = {
  entry: {
    app: [
      './src/main/javascript/index.js'
    ]
  },
  output: {
    path: path.resolve(__dirname, 'target/scala-2.11/classes/public'),
    filename: 'app.js'
  },
  devtool: 'sourcemap',
  module: {
    rules: [
      {
        test: /\.jsx?$/,
        use: [{
            loader: 'babel-loader',
            options: {
                cacheDirectory: true,
                presets: ['react', 'es2015', 'stage-0'],
                plugins: []
            }
        }],
        include: path.join(__dirname, 'src')
      },
      {
        test: /\.(less|css)/,
        use: ['style-loader', 'css-loader', 'less-loader']
      },
      {
        test: /\.(png|woff|woff2|eot|ttf|svg)(\?v=[0-9]\.[0-9]\.[0-9])?$/,
        use: 'file-loader'
      }
    ]
  },
  plugins: [
    new HtmlWebpackPlugin({
      template: './src/main/html/index.html',
      inject: 'body'
    }),
    new FaviconsWebpackPlugin({
      logo: './src/main/style/images/favicon.png',
      icons: {
        android: false,
        appleIcon: false,
        appleStartup: false,
        coast: false,
        favicons: true,
        firefox: false,
        opengraph: false,
        twitter: false,
        yandex: false,
        windows: false
      }
    }),
    new FlowStatusWebpackPlugin({
      binaryPath: './node_modules/.bin/flow',
      failOnError: true
    })
  ]
};
