// @flow

import React from "react";
import injectSheet from "react-jss";

import map from "lodash/map";
import find from "lodash/find";

import Dagger from "../../graph/Dagger";

import type { Node, Edge } from "../../graph/dagger/dataAPI/genericGraph";
import type { Workflow, Tag, Job, Dependency } from "../../datamodel";

import Window from "../components/Window";
import FancyTable from "../components/FancyTable";

type Props = {
  classes: any,
  workflow: Workflow,
  job: string
};

class WorkflowComponent extends React.Component {
  props: Props;

  render() {
    const { classes, workflow = {}, job } = this.props;
    const nodes: Node[] = map(workflow.jobs, (j: Job, i) => ({
      ...j,
      order: i,
      yPosition: i
    }));
    const edges: Edge[] = map(workflow.dependencies, (d: Dependency) => ({
      id: d.from + d.to,
      source: d.from,
      target: d.to,
      value: 1
    }));
    const tags: Tag[] = workflow.tags;
    const startNode = find(workflow.jobs, { id: job }) || workflow.jobs[0];
    return (
      <div className={classes.main}>
        <Dagger
          nodes={nodes}
          edges={edges}
          tags={tags}
          startNodeId={startNode.id}
        />
        <Window className={classes.nodeDescription} title="Job Information">
          <FancyTable key="infos">
            <dt key="id">Id:</dt>
            <dd key="id_">{startNode.id}</dd>
            <dt key="name">Name:</dt>
            <dd key="name_">{startNode.name}</dd>
            <dt key="description">Description:</dt>
            <dd key="description_">{startNode.description}</dd>
            <dt key="tags">Tags:</dt>
            <dd key="tags_">{map(startNode.tags, "name").join(", ")}</dd>
          </FancyTable>
        </Window>
      </div>
    );
  }
}

const styles = {
  main: {
    backgroundColor: "#ECF1F5",
    flex: 1,
    display: "flex",
    flexDirection: "row"
  },
  nodeDescription: {
    backgroundColor: "#FFF",
    flex: 1
  }
};

export default injectSheet(styles)(WorkflowComponent);
