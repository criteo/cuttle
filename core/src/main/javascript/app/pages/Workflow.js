// @flow

import React from "react";
import injectSheet from "react-jss";

import map from "lodash/map";
import find from "lodash/find";

import Dagger from "../../graph/Dagger";

import type { Node, Edge } from "../../graph/dagger/dataAPI/genericGraph";
import type { Workflow, Tag, Job, Dependency } from "../../datamodel";

import SlidePanel from "../components/SlidePanel";

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
        <SlidePanel>
          <div className={classes.jobCard}>
            <div className="jobTitle">
              {startNode.name +
                (startNode.name != startNode.id ? "(" + startNode.id + ")" : "")}
            </div>
            {startNode.description && <div className="jobDescription">
              {startNode.description}
            </div>}
            
          </div>
        </SlidePanel>
      </div>
    );
  }
}

const styles = {
  main: {
    backgroundColor: "#ECF1F5",
    flex: 1,
    display: "flex"
  },
  nodeDescription: {
    backgroundColor: "#FFF",
    flex: 1
  },
  jobCard: {
    padding: "1em",
    "& .jobTitle": {
      fontFamily: "Arial",
      fontSize: "2em",
      color: "black",
      
    },
    "& .jobDescription": {
      fontFamily: "Arial",
      fontSize: "0.9em",
      color: "#3B4254",
      marginTop: "1em"
    }
  }
};

export default injectSheet(styles)(WorkflowComponent);
