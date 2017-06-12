// @flow

import React from "react";
import injectSheet from "react-jss";

import map from "lodash/map";
import find from "lodash/find";
import filter from "lodash/filter";
import some from "lodash/some";
import includes from "lodash/includes";

import type { Node, Edge } from "../../graph/dagger/dataAPI/genericGraph";
import type { Workflow, Tag, Job, Dependency } from "../../datamodel";

import Select from "react-select";
import { navigate } from "redux-url";
import { connect } from "react-redux";

import Dagger from "../../graph/Dagger";
import SlidePanel from "../components/SlidePanel";

type Props = {
  classes: any,
  workflow: Workflow,
  selectedJobs: string[],
  job: string,
  navTo: () => void
};

class WorkflowComponent extends React.Component {
  props: Props;

  render() {
    const {
      classes,
      workflow = {},
      job,
      selectedJobs = [],
      navTo
    } = this.props;

    const filteredJobs = filter(workflow.jobs, j =>
      includes(selectedJobs, j.id)
    );
    const jobs = filteredJobs.length > 0 ? filteredJobs : workflow.jobs;
    const nodes: Node[] = map(jobs, (j: Job, i) => ({
      ...j,
      order: i,
      yPosition: i
    }));

    const filteredEdges = filter(
      workflow.dependencies,
      e => some(jobs, { id: e.from }) && some(jobs, { id: e.to })
    );
    const edges: Edge[] = map(filteredEdges, (d: Dependency) => ({
      id: d.from + d.to,
      source: d.from,
      target: d.to,
      value: 1
    }));
    const tags: Tag[] = workflow.tags;
    const startNode = find(jobs, { id: job }) || jobs[0];
    return (
      <div className={classes.main}>
        <Dagger
          nodes={nodes}
          edges={edges}
          tags={tags}
          startNodeId={startNode.id}
          onClickNode={id => navTo("/workflow/" + id)}
        />
        <Select
          className={classes.jobSelector}
          name="jobSelector"
          options={map(nodes, n => ({ value: n.id, label: n.name }))}
          onChange={o => navTo("/workflow/" + o.value)}
        />
        <SlidePanel>
          <div className={classes.jobCard}>
            <div className="jobTitle">
              {startNode.name +
                (startNode.name != startNode.id
                  ? "(" + startNode.id + ")"
                  : "")}
            </div>
            {startNode.description &&
              <div className="jobDescription">
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
    width: "100%",
    height: "calc(100vh - 4em)",
    position: "relative"
  },
  jobSelector: {
    position: "absolute",
    top: "2em",
    left: "50%",
    marginLeft: "-300px",
    width: "600px",
    "& .Select-control": {
      borderRadius: "1em",
      height: "1em",
      backgroundColor: "#F5F8FA",
      "& .Select-value": {
        color: "#A9B8C3",
        fontSize: "0.9em"
      },
      "& .Select-menu ! important": {
        margin: "0 1em",
        width: "calc(600px - 2em)"
      },
      "& .Select-menu-outer !important": {
        margin: "0 1em",
        width: "calc(600px - 2em)"
      },
      "& .Select-option !important": {
        fontSize: "0.9em"
      },
      "& .Select-arrow-zone": {
        display: "none"
      }
    }
  },
  jobCard: {
    padding: "1em",
    "& .jobTitle": {
      fontFamily: "Arial",
      fontSize: "2em",
      color: "black"
    },
    "& .jobDescription": {
      fontFamily: "Arial",
      fontSize: "0.9em",
      color: "#3B4254",
      marginTop: "1em"
    }
  }
};

export default connect(
  () => ({}),
  dispatch => ({
    navTo: link => dispatch(navigate(link))
  })
)(injectSheet(styles)(WorkflowComponent));
