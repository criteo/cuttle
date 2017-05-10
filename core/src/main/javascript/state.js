// @flow

import type { Action } from "./actions";
import type { Workflow } from "./datamodel/workflow";
import { prepareWorkflow } from "./datamodel/workflow";
import type Project from "./datamodel/project";
import type { Userbar } from "./datamodel/userbar";

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

type LoadingStructure<T> = {
  isLoading: boolean,
  data?: T
};

export type State = {
  page: PageId,
  workflow: LoadingStructure<Workflow>,
  project: LoadingStructure<Project>,
  userbar: Userbar,
  globalError?: string
};

export const initialState: State = {
  page: "monitoring",
  project: { isLoading: false },
  workflow: { isLoading: false },
  userbar: {
    open: false,
    selectedJobs: [],
    jobSearchInput: "",
    selectedTags: []
  }
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
        case "success":
          return {
            ...currentState,
            project: {
              data: action.data,
              isLoading: false
            }
          }
        case "pending":
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
              data: { name: "." },
              isLoading: false
            }
          }
      }
    }

    case "LOAD_WORKFLOW_DATA": {
      switch(action.status) {
        case "success":
          return {
            ...currentState,
            workflow: {
              data: prepareWorkflow(action.data),
              isLoading: false
            }
          }
        case "pending":
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
