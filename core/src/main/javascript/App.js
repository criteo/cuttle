// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";

import RightPane from "./components/RightPane";
import LeftPane from "./components/LeftPane";
import MenuHeader from "./components/menu/MenuHeader";
import Spinner from "./components/generic/Spinner";
import Menu from "./components/menu/Menu";
import type { PageId } from "./state";
import * as Actions from "./actions";

import Workflow from "./components/tabs/Workflow";
import UserBar from "./components/UserBar";

import reduce from "lodash/reduce";

type Props = {
  activeTab: PageId,
  projectName: string,
  environment: string,
  workflow: Workflow,
  isLoading: boolean,
  loadAppData: () => void,
  closeUserbar: () => void,
  classes: any
};

class App extends React.Component {
  props: Props;

  constructor(props: Props) {
    super(props);
  }

  componentDidMount() {
    this.props.loadAppData();
  }

  render() {
    const {
      classes,
      activeTab,
      environment,
      projectName,
      workflow,
      isLoading,
      closeUserbar
    } = this.props;

    if (isLoading) {
      return <Spinner className={classes.loader} />;
    } else {
      const allJobs = reduce(
        workflow.jobs,
        (acc, cur) => ({
          ...acc,
          [cur.id]: cur
        }),
        {}
      );

      const allTags = reduce(
        workflow.tags,
        (acc, cur) => ({
          ...acc,
          [cur.name]: cur
        }),
        {}
      );

      return (
        <div className={classes.main} onClick={closeUserbar}>
          <LeftPane className={classes.leftpane}>
            <MenuHeader environment={environment} projectName={projectName} />
            <Menu activeTab={activeTab} />
          </LeftPane>
          <RightPane className={classes.rightpane}>
            <UserBar
              className={classes.userBar}
              allTags={allTags}
              allJobs={allJobs}
            />
            {(activeTab === "workflow" && <Workflow workflow={workflow} />) ||
              <div />}
          </RightPane>
        </div>
      );
    }
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
  },
  loader: {
    padding: "25px"
  }
};

const mapStateToProps = ({ page, project, workflow, isLoading }) => ({
  activeTab: page,
  projectName: project && project.name,
  environment: "production",
  workflow,
  isLoading
});

const mapDispatchToProps = dispatch => ({
  closeUserbar: Actions.closeUserbar(dispatch),
  loadAppData: Actions.loadAppData(dispatch)
});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(App)
);
