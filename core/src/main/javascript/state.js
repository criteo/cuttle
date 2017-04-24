// @flow

import type { Action } from "./actions";

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
  globalError?: string
};

export const initialState: State = {
  page: "monitoring"
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
      
    default:
      console.log("Unhandled action %o", action);
      return currentState;
  }
};
