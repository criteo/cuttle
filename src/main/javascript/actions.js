// @flow


type Status = 'success' | 'error';
type Dispatch = (action: Action) => void;

export type Action = (
  INIT
);

type INIT = {type: 'INIT'};

export const init = (): INIT => ({type: 'INIT'});
