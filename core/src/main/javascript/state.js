// @flow

import * as Actions from "./actions";

export type Page = { id: string, name: string };

export type State = {
  page: Page,
  globalError?: string
};

export const initialState: State = {
  page: { name: "INIT", id: "INIT" }
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
        page: { id: "INIT", name: "INIT" }
      };
    }

    default:
      console.log("Unhandled action %o", action);
      return currentState;
  }
};
