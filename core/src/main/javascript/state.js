// @flow

import type { Action } from "./actions";
import type { Project, Workflow, Statistics, Userbar } from "./datamodel";

import { prepareWorkflow } from "./datamodel";
import includes from "lodash/includes";
import without from "lodash/without";

export type Page =
  | { id: "" }
  | { id: "workflow" }
  | {
      id: "executions/started",
      page?: number,
      sort?: string,
      order?: "asc" | "desc"
    }
  | {
      id: "executions/stuck",
      page?: number,
      sort?: string,
      order?: "asc" | "desc"
    }
  | {
      id: "executions/paused",
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
  | { id: "timeseries/calendar" }
  | { id: "timeseries/backfills" };

export type State = {
  page: Page,
  workflow: ?Workflow,
  project: ?Project,
  statistics: Statistics,
  userbar: Userbar,
  isLoading: boolean,
  globalError?: string
};

export const initialState: State = {
  isLoading: true,
  page: { id: "" },
  project: null,
  workflow: null,
  statistics: {
    running: 0,
    paused: 0,
    failing: 0
  },
  userbar: {
    open: false,
    selectedJobs: [],
    jobSearchInput: "",
    selectedTags: []
  }
};

// -- Reducers

export const reducers = (currentState: State, action: Action): State => {
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
        case "success":
          let [project, workflow] = action.data;
          return {
            ...currentState,
            project: project,
            workflow: prepareWorkflow(workflow),
            isLoading: false
          };
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

    case "SELECT_JOB": {
      const currentSelectedJobs = currentState.userbar.selectedJobs;
      return {
        ...currentState,
        userbar: {
          ...currentState.userbar,
          selectedJobs: includes(currentSelectedJobs, action.jobId)
            ? [...currentSelectedJobs]
            : [...currentSelectedJobs, action.jobId]
        }
      };
    }

    case "DESELECT_JOB": {
      return {
        ...currentState,
        userbar: {
          ...currentState.userbar,
          selectedJobs: without(currentState.userbar.selectedJobs, action.jobId)
        }
      };
    }

    case "TOGGLE_USERBAR": {
      return {
        ...currentState,
        userbar: {
          ...currentState.userbar,
          open: !currentState.userbar.open
        }
      };
    }

    case "OPEN_USERBAR": {
      return {
        ...currentState,
        userbar: {
          ...currentState.userbar,
          open: true
        }
      };
    }

    case "CLOSE_USERBAR": {
      return {
        ...currentState,
        userbar: {
          ...currentState.userbar,
          open: false
        }
      };
    }

    case "CHANGE_JOBSEARCH_INPUT": {
      return {
        ...currentState,
        userbar: {
          ...currentState.userbar,
          jobSearchInput: action.inputText
        }
      };
    }

    case "SELECT_FILTERTAG": {
      const currentSelectedTags = currentState.userbar.selectedTags;
      return {
        ...currentState,
        userbar: {
          ...currentState.userbar,
          selectedTags: includes(currentSelectedTags, action.tagName)
            ? [...currentSelectedTags]
            : [...currentSelectedTags, action.tagName]
        }
      };
    }

    case "DESELECT_FILTERTAG": {
      const currentSelectedTags = currentState.userbar.selectedTags;
      return {
        ...currentState,
        userbar: {
          ...currentState.userbar,
          selectedTags: without(currentSelectedTags, action.tagName)
        }
      };
    }

    case "TOGGLE_FILTERTAG": {
      const currentSelectedTags = currentState.userbar.selectedTags;
      return {
        ...currentState,
        userbar: {
          ...currentState.userbar,
          selectedTags: includes(currentSelectedTags, action.tagName)
            ? without(currentSelectedTags, action.tagName)
            : [action.tagName, ...currentSelectedTags]
        }
      };
    }

    default:
      console.log("Unhandled action %o", action);
      return currentState;
  }
};
