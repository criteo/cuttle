// @flow

import React from "react";
import injectSheet from "react-jss";

import map from "lodash/map";

import Dagger from "../../graph/Dagger";

import type { Node, Edge } from "../../graph/dagger/dataAPI/genericGraph";
import type { Workflow, Tag, Job, Dependency } from "../../datamodel";

type Props = {
  classes: any,
  workflow: Workflow
};

class WorkflowComponent extends React.Component {
  props: Props;

  render() {
    const { classes, workflow = {} } = this.props;
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
    return (
      <div className={classes.main}>
        <Dagger nodes={nodes} edges={edges} tags={tags} />
      </div>
    );
  }
}

const styles = {
  main: {
    backgroundColor: "#ECF1F5",
    flex: 1,
    display: "flex",
    flexDirection: "column"
  }
};

export default injectSheet(styles)(WorkflowComponent);
