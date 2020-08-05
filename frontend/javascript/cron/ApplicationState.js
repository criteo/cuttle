// @flow

import type { Action } from "./actions";
import type { Project } from "../common/datamodel";
import type { Workflow, Statistics } from "./datamodel";

import { prepareWorkflow } from "./datamodel";

export type DagsPage = {
  id: "dags",
  status: "all" | "active" | "paused",
  sort?: string,
  order?: "asc" | "desc"
};

export type Page =
  | { id: "" }
  | {
      id: "workflow",
      jobId?: string,
      showDetail: boolean,
      refPath?: string
    }
  | {
      id: "executions/started",
      page?: number,
      sort?: string,
      order?: "asc" | "desc"
    }
  | {
      id: "executions/retrying",
      page?: number,
      sort?: string,
      order?: "asc" | "desc"
    }
  | {
      id: "executions/finished",
      page?: number,
      sort?: string,
      order?: "asc" | "desc"
    }
  | { id: "executions/detail", execution: string }
  | DagsPage;

export type State = {
  page: Page,
  workflow: ?Workflow,
  project: ?Project,
  statistics: Statistics,
  isLoading: boolean,
  globalError?: string,
  selectedJobs: Array<string>
};

const initialState: State = {
  isLoading: true,
  page: { id: "" },
  project: null,
  workflow: null,
  statistics: {
    running: 0,
    waiting: 0,
    paused: 0,
    failing: 0,
    scheduler: []
  },
  selectedJobs: []
};

export const appReducer = (
  currentState: State = initialState,
  action: Action
): State => {
  switch (action.type) {
    case "OPEN_PAGE": {
      return {
        ...currentState,
        page: action.page
      };
    }

    case "UPDATE_STATISTICS": {
      return {
        ...currentState,
        statistics: action.statistics
      };
    }

    case "LOAD_APP_DATA": {
      switch (action.status) {
        case "success": {
          let [project, workflow] = action.data;
          return {
            ...currentState,
            project: project,
            workflow: prepareWorkflow(workflow),
            isLoading: false
          };
        }
        case "pending":
          return {
            ...currentState,
            isLoading: true
          };
        case "error":
          return {
            ...currentState,
            globalError: action.globalErrorMessage
          };
        default:
          return currentState;
      }
    }

    case "SELECT_JOBS": {
      return {
        ...currentState,
        selectedJobs: action.jobs
      };
    }

    default:
      return currentState;
  }
};
