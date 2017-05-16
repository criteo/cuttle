// @flow

import React from "react";
import injectSheet from "react-jss";

import map from "lodash/map";

import Dagger from "../dumb/Dagger";
import Spinner from "../generic/Spinner";

import type { Node, Edge } from "../../d3/dagger/dataAPI/genericGraph";
import type { Workflow, Tag, Job, Dependency } from "../../datamodel/workflow";

type Props = {
  classes: any,
  workflow: Workflow,
  isLoadingWorkflow: boolean
};

class WorkflowComponent extends React.Component {
  props: Props;

  render() {
    const { classes, workflow = {}, isLoadingWorkflow } = this.props;
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
        {isLoadingWorkflow || nodes.length == 0
          ? <Spinner dark />
          : <Dagger
              nodes={nodes}
              edges={edges}
              tags={tags}
            />
        }
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
