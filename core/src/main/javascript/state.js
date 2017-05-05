// @flow

import type { Action } from "./actions";
import type { Workflow } from "./datamodel/workflow";
import type Project from "./datamodel/project";

import includes from "lodash/includes";
import without from "lodash/without";

export type PageId =
  | "monitoring"
    | "execution"
    | "execution_running"
    | "execution_success"
    | "execution_failed"
    | "workflow"
    | "calendar"
    | "admin";

export interface Page {
  id: PageId,
  label: string
}

export type State = {
  page: PageId,
  workflow: Workflow,
  project: Project,
  selectedJobs: string[],
  userbarOpen: boolean,
  globalError?: string
};

export const initialState: State = {
  page: "monitoring",
  project: {},
  workflow: {},
  userbarOpen: false,
  selectedJobs: []
};

// -- Reducers

export const reducers = (
  currentState: State,
  action: Action
): State => {
  switch (action.type) {
    case "INIT": {
      return {
        ...currentState,
        page: "monitoring"
      };
    }

    case "NAVIGATE": {
      return {
        ...currentState,
        page: action.pageId
      };
    }

    case "LOAD_PROJECT_DATA": {
      switch(action.status) {
        case 'success':
          return {
            ...currentState,
            project: {
              data: action.data,
              isLoading: false
            }
          }
        case 'pending':
          return {
            ...currentState,
            project: {
              isLoading: true
            }
          }
        default:
          return {
            ...currentState,
            globalError: action.globalErrorMessage,
            project: {
              name: ".",
              isLoading: false
            }
          }
      }
    }

    case "LOAD_WORKFLOW_DATA": {
      switch(action.status) {
        case 'success':
          return {
            ...currentState,
            workflow: {
              data: action.data,
              isLoading: false
            }
          }
        case 'pending':
          return {
            ...currentState,
            workflow: {
              isLoading: true
            }
          }
        default:
          return {
            ...currentState,
            globalError: action.globalErrorMessage,
            workflow: {
              isLoading: false
            }
          }
      }
    }

    case "SELECT_JOB": {
      return {
        ...currentState,
        selectedJobs: includes(currentState.selectedJobs, action.jobId)
          ? [...currentState.selectedJobs]
          : [action.jobId, ...currentState.selectedJobs]
      };
    }

    case "DESELECT_JOB": {
      return {
        ...currentState,
        selectedJobs: without(currentState.selectedJobs, action.jobId)
      };
    }

    case "TOGGLE_USERBAR": {
      return {
        ...currentState,
        userbarOpen: !currentState.userbarOpen
      };
    }
      
    default:
      console.log("Unhandled action %o", action);
      return currentState;
  }
};
