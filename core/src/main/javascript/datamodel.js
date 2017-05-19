//@flow
import map from "lodash/map";
import some from "lodash/some";

export type Project = { name: string, description: string };

export type Statistics = {
  running: number,
  paused: number,
  failing: number
};

export type Userbar = {
  selectedJobs: string[],
  jobSearchInput: string,
  selectedTags: string[],
  open: boolean
};

export type Dependency = { from: string, to: string };

export type Tag = { name: string, description: string };

export type NodeKind = "root" | "leaf" | "common";

export type Job = {
  id: string,
  name: string,
  description: string,
  tags: string[],
  kind?: NodeKind
};

export type Workflow = { jobs: Job[], dependencies: Dependency[], tags: Tag[] };

// We enrich the workflow with information in this method
// (if a job is root in the graph, or a leaf etc.)
export const prepareWorkflow = (w: Workflow): Workflow => ({
  ...w,
  jobs: map(w.jobs, j => ({
    ...j,
    kind: some(w.dependencies, { to: j.id })
      ? some(w.dependencies, { from: j.id }) ? "common" : "leaf"
      : "root"
  }))
});
