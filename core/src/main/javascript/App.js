// @flow

import React from "react";
import { connect } from "react-redux";
import { navigate } from "redux-url";
import injectSheet from "react-jss";

import RightPane from "./components/RightPane";
import LeftPane from "./components/LeftPane";
import MenuHeader from "./components/menu/MenuHeader";
import Menu from "./components/menu/Menu";
import type { PageId } from "./state";
import * as Actions from "./actions";

import WorkflowContainer from "./tabs/WorkflowContainer";

type Props = {
  activeTab: PageId,
  workflowName: string,
  environment: string,
  loadProjectData: any,
  isLoadingProject: boolean,
  classes: any
};

class App extends React.Component {
  props: Props;

  constructor(props: Props) {
    super(props);
    if (!this.props.isLoadedProject && !this.props.isLoadingProject)
      this.props.loadProjectData();
  }

  render() {
    const {
      classes,
      activeTab,
      environment,
      workflowName,
      isLoadingProject = true
    } = this.props;
    return (
      <div className={classes.main}>
        <RightPane className={classes.rightpane}>
          {(activeTab === "workflow" && <WorkflowContainer />) || <div />}
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
    width: "20%"
  },
  rightpane: {
    paddingLeft: "20%",
    width: "80%",
    position: "absolute"
  },
  main: {
    backgroundColor: "#ECF1F5"
  }
};

const mapStateToProps = ({ page, project = {} }) => ({
  activeTab: page,
  isLoadedProject: !!project.data,
  isLoadingProject: project.isLoading,
  workflowName: project.data && project.data.name,
  environment: "env"
});
const mapDispatchToProps = dispatch => ({
  loadProjectData: () => Actions.loadProjectData(dispatch)
});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(App)
);
