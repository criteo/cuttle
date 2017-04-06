// @flow

import * as Actions from "./actions";

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
  action: Actions.Action
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

    default:
      console.log("Unhandled action %o", action);
      return currentState;
  }
};
