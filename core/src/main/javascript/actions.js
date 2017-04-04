// @flow

type Status = "success" | "error";
type Dispatch = number;

type INIT = { type: "INIT" };

export type Action = INIT;

export const init = (): INIT => ({ type: "INIT" });
