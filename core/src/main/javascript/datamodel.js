//@flow
import _ from "lodash";
import moment, { Moment } from "moment";

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

export type ExecutionLog = {
  id: string,
  job: string,
  startTime: ?string,
  endTime: ?string,
  context: any,
  status: ExecutionStatus,
  failing?: {
    failedExecutions: Array<ExecutionLog>,
    nextRetry: ?string
  },
  waitingSeconds: number
};

export type Paginated<A> = {
  total: number,
  data: Array<A>,
  completion?: number
};

export type Statistics = {
  running: number,
  waiting: number,
  paused: number,
  failing: number,
  scheduler?: any,
  error?: boolean
};

export type Dependency = { from: string, to: string };

export type Tag = { name: string, description: string };

export type NodeKind = "root" | "leaf" | "common";

export type Scheduling =
  | {
      grid: {
        period: "daily",
        zoneId: string
      },
      start: string,
      maxPeriods: number
    }
  | {
      grid: {
        period: "hourly" | "continuous"
      },
      start: string,
      maxPeriods: number
    };

export type Job = {
  id: string,
  name: string,
  description: string,
  scheduling: Scheduling,
  tags: string[],
  kind?: NodeKind
};

export type Workflow = {
  jobs: Job[],
  dependencies: Dependency[],
  tags: Tag[],
  getJob: (id: string) => ?Job,
  getParents: (id: string) => Array<string>,
  getChildren: (id: string) => Array<string>,
  getTagged: (tag: string) => Array<string>
};

// We enrich the workflow with information in this method
// (if a job is root in the graph, or a leaf etc.)
export const prepareWorkflow = (w: Workflow): Workflow => ({
  ...w,
  jobs: _.map(w.jobs, j => ({
    ...j,
    kind: _.some(w.dependencies, { to: j.id })
      ? _.some(w.dependencies, { from: j.id }) ? "common" : "leaf"
      : "root"
  })),
  getJob(id: string) {
    return this.jobs.find(job => job.id == id);
  },
  getParents(id: string) {
    let parents = this.dependencies
      .filter(({ to }) => to == id)
      .map(({ from }) => from);
    return parents.concat(_.flatMap(parents, this.getParents.bind(this)));
  },
  getChildren(id: string) {
    let childrens = this.dependencies
      .filter(({ from }) => from == id)
      .map(({ to }) => to);
    return childrens.concat(_.flatMap(childrens, this.getChildren.bind(this)));
  },
  getTagged(tag: string) {
    return this.jobs
      .filter(job => job.tags.indexOf(tag) > -1)
      .map(job => job.id);
  }
});

export type Backfill = {
  id: string,
  jobs: Array<string>,
  name: string,
  description: string,
  start: Moment,
  end: Moment,
  created_at: Moment,
  created_by: string,
  priority: number,
  status: string
};

export const backfillFromJSON = (json: any): Backfill => ({
  ...json,
  jobs: json.jobs.split(","),
  start: moment.utc(json.start),
  end: moment.utc(json.end),
  created_at: moment.utc(json.created_at)
});
