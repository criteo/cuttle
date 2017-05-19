// @flow

import React from "react";

import createHistory from "history/createBrowserHistory"; // choose a history implementation
import { createStore, compose, applyMiddleware } from "redux";
import { Provider } from "react-redux";
import { createRouter, navigate } from "redux-url";
import { render } from "react-dom";
import ReduxThunk from "redux-thunk";

import "../style/index.less";

import App from "./App";
import { initialState, reducers } from "./state";
import * as Actions from "./actions";
import type { Statistics } from "./datamodel";
import { listenEvents } from "./Utils";

import { navigToPage } from "./actions";

const routes = {
  "/": () => navigToPage("executions/started"),
  "/executions/started": () => navigToPage("executions/started"),
  "/executions/stuck": () => navigToPage("executions/stuck"),
  "/executions/finished": () => navigToPage("executions/finished"),
  "/executions/paused": () => navigToPage("executions/paused"),
  "/workflow": () => navigToPage("workflow"),
  "/timeseries/calendar": () => navigToPage("timeseries/calendar"),
  "/timeseries/backfill": () => navigToPage("timeseries/backfill")
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

store.dispatch(navigate(location.pathname, true));
store.dispatch(Actions.loadAppData());
listenEvents("/api/statistics?stream=true", stats =>
  store.dispatch(Actions.updateStatistics(stats)));

render(
  <Provider store={store}>
    <App />
  </Provider>,
  document.getElementById("app")
);
