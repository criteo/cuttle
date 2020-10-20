//@flow
import _ from "lodash";

import type { ExecutionStatus, NodeKind, Tag } from "../common/datamodel";

export type CronContext = {
  instant: string
};

export type ExecutionLog = {
  id: string,
  job: string,
  startTime: ?string,
  endTime: ?string,
  context: CronContext,
  status: ExecutionStatus,
  failing?: {
    failedExecutions: Array<ExecutionLog>,
    nextRetry: ?string
  },
  waitingSeconds: number
};

export type DagStatus = "waiting" | "running" | "paused";

export type DagState = {
  id: string,
  status: DagStatus,
  nextInstant?: string,
  pausedUser?: string,
  pausedDate?: string
};

export type Statistics = {
  running: number,
  waiting: number,
  paused: number,
  failing: number,
  scheduler: DagState[],
  error?: boolean
};

export type Dependency = { from: string, to: string };

export type DagScheduling = {
  expression: string,
  tz: string
};

export type DagPipeline = {
  vertices: [string],
  edges: [Dependency]
};

export type Dag = {
  id: string,
  name: string,
  description: string,
  expression: DagScheduling,
  tags: string[],
  pipeline: DagPipeline
};

export type JobScheduling = {
  maxRetry: number
};

export type Job = {
  id: string,
  name: string,
  description: string,
  scheduling: JobScheduling,
  tags: string[],
  kind?: NodeKind
};

export type Workflow = {
  dags: Dag[],
  jobs: Job[],
  dependencies: Dependency[],
  tags: Tag[],
  getJob: (id: string) => ?Job,
  getJobParents: (id: string) => Array<string>,
  getJobChildren: (id: string) => Array<string>,
  getDagFromJob: (id: string) => ?Dag,
  getDag: (id: string) => ?Dag,
  getTagged: (tag: string) => Array<string>
};

// We enrich the workflow with information in this method
// (if a job is root in the graph, or a leaf etc.)
export const prepareWorkflow = (w: Workflow): Workflow => ({
  ...w,
  jobs: _.map(w.jobs, j => ({
    ...j,
    kind: _.some(w.dependencies, { to: j.id })
      ? _.some(w.dependencies, { from: j.id })
        ? "common"
        : "leaf"
      : "root"
  })),
  getJob(id: string) {
    return this.jobs.find(job => job.id == id);
  },
  getJobParents(id: string) {
    let parents = this.dependencies
      .filter(({ to }) => to == id)
      .map(({ from }) => from);
    return parents.concat(_.flatMap(parents, this.getJobParents.bind(this)));
  },
  getJobChildren(id: string) {
    let childrens = this.dependencies
      .filter(({ from }) => from == id)
      .map(({ to }) => to);
    return childrens.concat(
      _.flatMap(childrens, this.getJobChildren.bind(this))
    );
  },
  getDagFromJob(id: string) {
    return this.dags.find(dag => dag.pipeline.vertices.includes(id));
  },
  getDag(id: string) {
    return this.dags.find(dag => dag.id == id);
  },
  getTagged(tag: string) {
    return this.jobs
      .filter(job => job.tags.indexOf(tag) > -1)
      .map(job => job.id);
  }
});
