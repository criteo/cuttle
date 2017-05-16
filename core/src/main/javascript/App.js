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

import Workflow from "./components/tabs/Workflow";
import UserBar from "./components/UserBar";

import reduce from "lodash/reduce";

type Props = {
  activeTab: PageId,
  workflowName: string,
  environment: string,
  workflow: Workflow,
  
  isLoadedProject: boolean,
  isLoadingProject: boolean,
  loadProjectData: () => void,
  
  isLoadingWorkflow: boolean,
  isWorkflowLoaded: boolean,
  loadWorkflowData: () => void,

  closeUserbar: () => void,
  
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
      closeUserbar
    } = this.props;

    const workflowAvailable = !isLoadingWorkflow && workflow;

    const allJobs = workflowAvailable &&
      reduce(
        workflow.jobs,
        (acc, cur) => ({
          ...acc,
          [cur.id]: cur
        }),
        {}
      ) || {};

    const allTags = workflowAvailable &&
      reduce(
        workflow.tags,
        (acc, cur) => ({
          ...acc,
          [cur.name]: cur
        }),
        {}
      ) || {};

    return (
      <div
        className={classes.main}
        onClick={closeUserbar}
      >
        <LeftPane className={classes.leftpane}>
          <MenuHeader
            environment={environment}
            workflowName={workflowName}
            isLoading={isLoadingProject}
          />
          <Menu activeTab={activeTab} />
        </LeftPane>
        <RightPane className={classes.rightpane}>
          <UserBar
            className={classes.userBar}
            allTags={allTags}
            allJobs={allJobs}
            isLoading={isLoadingWorkflow}
          />
          {(activeTab === "workflow" &&
            <Workflow
              workflow={workflow}
              isLoadingWorkflow={isLoadingWorkflow}
            />) ||
            <div />}
        </RightPane>
      </div>
    );
  }
}

let styles = {
  leftpane: {
    width: "300px",
    display: "flex",
    flexDirection: "column"
  },
  rightpane: {
    display: "flex",
    flexGrow: 1,
    alignItems: "stretch",
    flexDirection: "column"
  },
  userBar: {
    zIndex: 2
  },
  main: {
    backgroundColor: "#ECF1F5",
    display: "flex",
    alignItems: "stretch"
  }
};

const mapStateToProps = ({ page, project, workflow }) => ({
  activeTab: page,
  isLoadedProject: !!project.data,
  isLoadingProject: project.isLoading,
  workflowName: project.data && project.data.name,
  environment: "env",
  isLoadingWorkflow: workflow.isLoading,
  isWorkflowLoaded: !!workflow.data,
  workflow: workflow.data
});

const mapDispatchToProps = dispatch => ({
  loadProjectData: Actions.loadProjectData(dispatch),
  loadWorkflowData: Actions.loadWorkflowData(dispatch),
  
  closeUserbar: Actions.closeUserbar(dispatch)
});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(App)
);
