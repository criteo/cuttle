// @flow

export type Project = {
  name: string,
  version: ?string,
  description: ?string,
  env: {
    name: ?string,
    critical: boolean
  }
};

export type ExecutionStatus = "running" | "throttled";

export type Tag = { name: string, description: string };

export type Tags = { [string]: string };

export type NodeKind = "root" | "leaf" | "common";

export type Paginated<A> = {
  total: number,
  data: Array<A>,
  completion?: number
};
