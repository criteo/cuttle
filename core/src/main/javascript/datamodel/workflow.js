export type Dependency = { from: string, to: string };

export type Tag = { name: string, description: string };

export type Job = {
  id: string,
  name: string,
  description: string,
  tags: string[]
};

export type Workflow = { jobs: Job[], dependencies: Dependency[], tags: Tag[] };
