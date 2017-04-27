// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";

import RightPane from "./components/RightPane";
import LeftPane from "./components/LeftPane";
import MenuHeader from "./components/menu/MenuHeader";
import Menu from "./components/menu/Menu";
import type { PageId } from "./state";
import * as Actions from "./actions";

import Spinner from "./components/generic/Spinner";
import Workflow from "./components/tabs/Workflow";
import UserBar from "./components/UserBar";
import JobFilterForm from "./components/JobFilterForm";

import reduce from "lodash/reduce";

type Props = {
  activeTab: PageId,
  workflowName: string,
  environment: string,
  workflow: Workflow,
  loadProjectData: () => void,
  isLoadedProject: boolean,
  isLoadingProject: boolean,
  isLoadingWorkflow: boolean,
  isWorkflowLoaded: boolean,
  loadWorkflowData: () => void,
  selectJob: () => void,
  deselectJob: () => void,
  selectedJobs: string[],
  classes: any
};

class App extends React.Component {
  props: Props;

  constructor(props: Props) {
    super(props);
    if (!this.props.isLoadedProject && !this.props.isLoadingProject)
      this.props.loadProjectData();
    if (!this.props.isWorkflowLoaded && !this.props.isLoadingWorkflow)
      this.props.loadWorkflowData();
  }

  render() {
    const {
      classes,
      activeTab,
      environment,
      workflowName,
      workflow,
      isLoadingWorkflow = true,
      isLoadingProject = true,
      selectJob,
      deselectJob,
      selectedJobs
    } = this.props;

    const allJobs = !isLoadingWorkflow && reduce(workflow.jobs, (acc, cur) => ({
      ...acc,
      [cur.id]: cur
    }), {});

    const allTags = !isLoadingWorkflow && reduce(workflow.tags, (acc, cur) => ({
      ...acc,
      [cur.name]: cur
    }), {});

    return (
      <div className={classes.main}>
        <RightPane className={classes.rightpane}>
          <UserBar className={classes.userBar} selectedJobs={selectedJobs} allJobs={allJobs}>
            {isLoadingWorkflow
              ? <Spinner dark key="spinner-userbar" />
              : <JobFilterForm
                  key="job-filter-form"
                  allJobs={allJobs}
                  allTags={allTags}
                  selectJob={selectJob}
                  deselectJob={deselectJob}
                  selectedJobs={selectedJobs}
                />}
          </UserBar>
          {(activeTab === "workflow" &&
            <Workflow
              workflow={workflow}
              isLoadingWorkflow={isLoadingWorkflow}
            />) ||
            <div />}
        </RightPane>
        <LeftPane className={classes.leftpane}>
          <MenuHeader
            environment={environment}
            workflowName={workflowName}
            isLoading={isLoadingProject}
          />
          <Menu activeTab={activeTab} />
        </LeftPane>
      </div>
    );
  }
}

let styles = {
  leftpane: {
    position: "fixed",
    width: "20vw",
    maxWidth: "300px"
  },
  rightpane: {
    paddingLeft: "20vw",
    width: "80vw",
    position: "absolute"
  },
  userBar: {
    width: "80vw",
    position: "absolute"
  },
  main: {
    backgroundColor: "#ECF1F5"
  }
};

const mapStateToProps = ({ page, project = {}, workflow = {}, selectedJobs }) => ({
  activeTab: page,
  isLoadedProject: !!project.data,
  isLoadingProject: project.isLoading,
  workflowName: project.data && project.data.name,
  environment: "env",
  isLoadingWorkflow: workflow.isLoading,
  isWorkflowLoaded: !!workflow.data,
  workflow: workflow.data,
  selectedJobs: selectedJobs
});
const mapDispatchToProps = dispatch => ({
  loadProjectData: () => Actions.loadProjectData(dispatch),
  loadWorkflowData: () => Actions.loadWorkflowData(dispatch),
  selectJob: Actions.selectJob(dispatch),
  deselectJob: Actions.deselectJob(dispatch)
});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(App)
);
