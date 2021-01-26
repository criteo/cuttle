const process = require("process");
const path = require("path");
const HtmlWebpackPlugin = require("html-webpack-plugin");
const FlowStatusWebpackPlugin = require("flow-status-webpack-plugin");
const merge = require("webpack-merge");
const args = require("yargs").argv;
const project = args.project;

const env = process.env.NODE_ENV || "production";

const outputPath = path.resolve(
  __dirname,
  `${project}/target/scala-2.11/classes/public/${project}`
);

const common = {
  entry: {
    app: [path.resolve(__dirname, `frontend/javascript/${project}/index.js`)]
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
    publicPath: `/public/${project}/`
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
              presets: ["@babel/env", "@babel/flow", "@babel/react"],
              plugins: ["@babel/plugin-proposal-class-properties"]
            }
          }
        ],
        include: path.join(__dirname, "frontend"),
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
      template: path.resolve(__dirname, "frontend/html/index.html"),
      inject: "body"
    }),
    new FlowStatusWebpackPlugin({
      binaryPath: path.resolve(__dirname, "node_modules/.bin/flow"),
      failOnError: true
    })
  ]
};

module.exports = merge(common, {
  mode: env
});
