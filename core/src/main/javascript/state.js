// @flow

import type { Action } from "./actions";
import type { Workflow } from "./datamodel/workflow";
import { prepareWorkflow } from "./datamodel/workflow";
import type Project from "./datamodel/project";
import type { Userbar } from "./datamodel/userbar";

import includes from "lodash/includes";
import without from "lodash/without";

export type PageId =
  | "workflow"
  | "executions/running"
  | "executions/stuck"
  | "executions/paused"
  | "executions/finished";

export type Page = {
  id: PageId,
  label: string
};

export type State = {
  page: PageId,
  workflow: ?Workflow,
  project: ?Project,
  userbar: Userbar,
  isLoading: boolean,
  globalError?: string
};

export const initialState: State = {
  page: "workflow",
  project: null,
  workflow: null,
  userbar: {
    open: false,
    selectedJobs: [],
    jobSearchInput: "",
    selectedTags: []
  },
  isLoading: true
};

// -- Reducers

export const reducers = (currentState: State, action: Action): State => {
  switch (action.type) {
    case "INIT": {
      return {
        ...currentState,
        page: "workflow"
      };
    }

    case "NAVIGATE": {
      return {
        ...currentState,
        page: action.pageId
      };
    }

    case "LOAD_APP_DATA": {
      switch (action.status) {
        case "success":
          let [project, workflow] = action.data;
          return {
            ...currentState,
            project: project,
            workflow: workflow,
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
