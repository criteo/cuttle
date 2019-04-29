const path = require("path");
const HtmlWebpackPlugin = require("html-webpack-plugin");
const FlowStatusWebpackPlugin = require("flow-status-webpack-plugin");
const UglifyJSPlugin = require("uglifyjs-webpack-plugin");

const outputPath = path.resolve(
  __dirname,
  "timeseries/target/scala-2.11/classes/public"
);

module.exports = {
  entry: {
    app: [path.resolve(__dirname, "timeseries/src/main/javascript/index.js")]
  },

  devServer: {
    contentBase: outputPath,
    historyApiFallback: true,
    port: 9000,
    proxy: {
      "/api": "http://localhost:8888"
    }
  },

  output: {
    filename: "[name].js",
    path: outputPath,
    publicPath: "/public/"
  },

  devtool: "sourcemap",

  module: {
    rules: [
      {
        test: /\.jsx?$/,
        use: [
          {
            loader: "babel-loader",
            options: {
              cacheDirectory: true,
              presets: ["react", "es2015", "stage-0"],
              plugins: []
            }
          }
        ],
        include: path.join(__dirname, "timeseries/src"),
        exclude: [path.resolve(__dirname, "timeseries/node_modules/")]
      },
      {
        test: /\.(less|css)/,
        use: ["style-loader", "css-loader", "less-loader"]
      },
      {
        test: /\.(jpe?g|png|woff|woff2|eot|ttf|svg)(\?v=[0-9]\.[0-9]\.[0-9])?$/,
        use: "file-loader?name=[name].[ext]"
      }
    ]
  },

  plugins: [
    new HtmlWebpackPlugin({
      template: path.resolve(__dirname, "timeseries/src/main/html/index.html"),
      inject: "body"
    }),
    new FlowStatusWebpackPlugin({
      binaryPath: path.resolve(__dirname, "node_modules/.bin/flow"),
      failOnError: true
    }),
    new UglifyJSPlugin({
      mangle: false
    })
  ]
};
