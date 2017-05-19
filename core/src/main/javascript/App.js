// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";

import MenuHeader from "./app/menu/MenuHeader";
import Spinner from "./app/generic/Spinner";
import Menu from "./app/menu/Menu";
import type { PageId } from "./state";
import * as Actions from "./actions";

import Workflow from "./app/tabs/Workflow";
import UserBar from "./app/filter/UserBar";
import type { Statistics } from "./datamodel";

import reduce from "lodash/reduce";

type Props = {
  activeTab: PageId,
  projectName: string,
  environment: string,
  workflow: Workflow,
  statistics: Statistics,
  isLoading: boolean,
  closeUserbar: () => void,
  classes: any
};

class App extends React.Component {
  props: Props;

  constructor(props: Props) {
    super(props);
  }

  render() {
    const {
      classes,
      activeTab,
      environment,
      projectName,
      workflow,
      isLoading,
      closeUserbar,
      statistics
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
          <section className={classes.leftpane}>
            <MenuHeader environment={environment} projectName={projectName} />
            <Menu activeTab={activeTab} statistics={statistics} />
          </section>
          <section className={classes.rightpane}>
            <UserBar
              className={classes.userBar}
              allTags={allTags}
              allJobs={allJobs}
            />
            {(activeTab === "workflow" && <Workflow workflow={workflow} />) ||
              <div />}
          </section>
        </div>
      );
    }
  }
}

let styles = {
  leftpane: {
    width: "300px",
    display: "flex",
    flexDirection: "column",
    backgroundColor: "#2F3647",
    color: "#758390",
    fontFamily: "Arial",
    height: "100vh",
    zIndex: 100
  },
  rightpane: {
    display: "flex",
    flexGrow: 1,
    alignItems: "stretch",
    flexDirection: "column",
    backgroundColor: "#ECF1F5",
    color: "#3D4454",
    fontFamily: "Arial"
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

const mapStateToProps = (
  { page, project, workflow, isLoading, statistics }
) => ({
  activeTab: page,
  projectName: project && project.name,
  environment: "production",
  workflow,
  isLoading,
  statistics
});

const mapDispatchToProps = dispatch => ({
  closeUserbar: Actions.closeUserbar(dispatch)
});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(App)
);
