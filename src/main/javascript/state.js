// @flow

import * as Actions from './actions';
import _ from 'lodash';

export type Page =  {id: string, name: string};

export type State = {
  page: Page,
  globalError?: string
};

export const initialState: State = {
  page: {name: 'start', id: 'start'}
};

// -- Reducers

export const reducers = (currentState: State, action: Actions.Action): State => {
  switch(action.type) {
    case 'INIT': {
      return {
        ...currentState,
        page: {id: 'init', name: 'init'}
      }
    }

    default:
      console.log('Unhandled action %o', action);
      return currentState;
  }
};
