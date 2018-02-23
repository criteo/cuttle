// @flow

import React from "react";

import createHistory from "history/createBrowserHistory"; // choose a history implementation
import { createStore, combineReducers, compose, applyMiddleware } from "redux";
import { Provider } from "react-redux";
import { createRouter } from "redux-url";
import { render } from "react-dom";
import ReduxThunk from "redux-thunk";
import { reducer as formReducer } from "redux-form";

import "../style/index.less";

import App from "./App";
import { appReducer } from "./ApplicationState";
import * as Actions from "./actions";
import type { State } from "./ApplicationState";
import { listenEvents } from "./Utils";

import { openPage } from "./actions";

const routes = {
  "/": () => openPage({ id: "executions/started" }),
  "/executions/started": (_, { page, sort, order }) =>
    openPage({ id: "executions/started", page, sort, order }),
  "/executions/stuck": (_, { page, sort, order }) =>
    openPage({ id: "executions/stuck", page, sort, order }),
  "/executions/finished": (_, { page, sort, order }) =>
    openPage({ id: "executions/finished", page, sort, order }),
  "/executions/paused": (_, { page, sort, order }) =>
    openPage({ id: "executions/paused", page, sort, order }),
  "/executions/:id": ({ id }) =>
    openPage({ id: "executions/detail", execution: id }),
  "/workflow/*": ({ _ }, { showDetail }) =>
    openPage({ id: "workflow", jobId: _, showDetail: Boolean(showDetail) }),
  "/workflow": () => openPage({ id: "workflow", showDetail: false }),
  "/timeseries/calendar": () => openPage({ id: "timeseries/calendar" }),
  "/timeseries/calendar/:start_:end": ({ start, end }) =>
    openPage({ id: "timeseries/calendar/focus", start, end }),
  "/timeseries/backfills": (_, { page, sort, order }) =>
    openPage({ id: "timeseries/backfills", page, sort, order }),
  "/timeseries/backfills/create": () =>
    openPage({ id: "timeseries/backfills/create" }),
  "/timeseries/backfills/:backfillId": (
    { backfillId },
    { page, sort, order }
  ) =>
    openPage({
      id: "timeseries/backfills/detail",
      backfillId,
      page,
      sort,
      order
    }),
  "/timeseries/executions/*": ({ _ }) => {
    return openPage({
      id: "timeseries/executions",
      ...parseExecutionsRoute(_)
    });
  },
  "/jobs/all": (_, { sort, order }) =>
    openPage({ id: "jobs/all", sort, order }),
  "/jobs/active": (_, { sort, order }) =>
    openPage({ id: "jobs/active", sort, order }),
  "/jobs/paused": (_, { sort, order }) =>
    openPage({ id: "jobs/paused", sort, order })
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
let statisticsQuery = null, statisticsListener = null, statisticsError = null;
let listenForStatistics = (query: string) => {
  if (query != statisticsQuery) {
    statisticsListener && statisticsListener.close();
    statisticsListener = listenEvents(
      query,
      stats => {
        if (statisticsError) {
          clearTimeout(statisticsError);
          statisticsError = undefined;
        }
        store.dispatch(Actions.updateStatistics(stats));
      },
      error => {
        if (!statisticsError) {
          statisticsError = setTimeout(
            () =>
              store.dispatch(
                Actions.updateStatistics({
                  running: 0,
                  paused: 0,
                  failing: 0,
                  waiting: 0,
                  scheduler: null,
                  error: true
                })
              ),
            15000
          );
        }
      }
    );
    statisticsQuery = query;
  }
};
store.subscribe(() => {
  let state: State = store.getState().app;
  let jobsFilter = state.selectedJobs.length
    ? `&jobs=${state.selectedJobs.join(",")}`
    : "";
  listenForStatistics(`/api/statistics?events=true${jobsFilter}`);
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
  document.getElementById("app")
);
