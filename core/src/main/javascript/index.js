// @flow

import React from "react";

import createHistory from "history/createBrowserHistory"; // choose a history implementation
import { createStore, compose, applyMiddleware } from "redux";
import { Provider } from "react-redux";
import { createRouter, navigate } from "redux-url";
import { render } from "react-dom";
import ReduxThunk from "redux-thunk";
import "../style/index.less";

import App from "./layout/app";
import { initialState, reducers } from "./state";

const routes = {
  "/": "INIT"
};

const router = createRouter(routes, createHistory());
const store = createStore(
  reducers,
  initialState,
  compose(
    applyMiddleware(router, ReduxThunk),
    window.devToolsExtension ? window.devToolsExtension() : _ => _
  )
);

render(
  <Provider store={store}>
    <App />
  </Provider>,
  document.getElementById("app")
);
