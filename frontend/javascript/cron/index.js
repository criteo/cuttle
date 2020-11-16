// @flow

import React from "react";

import createHistory from "history/createBrowserHistory"; // choose a history implementation
import { createStore, combineReducers, compose, applyMiddleware } from "redux";
import { Provider } from "react-redux";
import { createRouter } from "redux-url";
import { render } from "react-dom";
import ReduxThunk from "redux-thunk";
import { reducer as formReducer } from "redux-form";
import isEqual from "lodash/isEqual";

import "../../style/index.less";

import App from "./App";
import { appReducer } from "./ApplicationState";
import * as Actions from "./actions";
import type { State } from "./ApplicationState";
import { PostEventSource } from "../common/Utils";

import { openPage } from "./actions";

const routes = {
  "/": (_, { page, sort, order }) =>
    openPage({ id: "executions/started", page, sort, order }),
  "/executions/started": (_, { page, sort, order }) =>
    openPage({ id: "executions/started", page, sort, order }),
  "/executions/retrying": (_, { page, sort, order }) =>
    openPage({ id: "executions/retrying", page, sort, order }),
  "/executions/finished": (_, { page, sort, order }) =>
    openPage({ id: "executions/finished", page, sort, order }),
  "/executions/:id": ({ id }) =>
    openPage({ id: "executions/detail", execution: id }),
  "/dags/:status": ({ status }, { sort, order }) =>
    openPage({ id: "dags", status, sort, order })
};

const parseExecutionsRoute = (() => {
  const executionsRouteRegex = /([^/]+)\/([^_]+)_([^/?#]+).*/;

  return (queryString: string) => {
    const match = executionsRouteRegex.exec(queryString);
    return (
      match && {
        job: match[1],
        start: match[2],
        end: match[3]
      }
    );
  };
})();

const router = createRouter(routes, createHistory());
const store = createStore(
  combineReducers({
    app: appReducer,
    form: formReducer
  }),
  compose(
    applyMiddleware(router, ReduxThunk),
    window.devToolsExtension ? window.devToolsExtension() : _ => _
  )
);

router.sync();
store.dispatch(Actions.loadAppData());

// Global stats listener
let selectedJobs = null,
  eventSource = null;

let listenForStatistics = (state: State) => {
  const jobs = state.selectedJobs;
  if (!isEqual(jobs, selectedJobs)) {
    const query = `/api/statistics`;
    eventSource && eventSource.stopPolling();
    eventSource = new PostEventSource(query, { jobs: jobs });
    eventSource.onmessage(stats => {
      store.dispatch(Actions.updateStatistics(stats.data));
    });
    eventSource.startPolling();
    selectedJobs = jobs;
  }
};

store.subscribe(() => {
  let state: State = store.getState().app;
  listenForStatistics(state);
  if (state.project && state.project.name) {
    if (state.project.env.name) {
      document.title = `${state.project.name} – ${state.project.env.name}`;
    } else {
      document.title = state.project.name;
    }
  }
});

render(
  <Provider store={store}>
    <App />
  </Provider>,
  (document.getElementById("app"): any)
);
