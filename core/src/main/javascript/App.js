// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";

import MenuHeader from "./app/menu/MenuHeader";
import Spinner from "./app/components/Spinner";
import Menu from "./app/menu/Menu";
import Link from "./app/components/Link";
import type { Page } from "./state";
import * as Actions from "./actions";

import Workflow from "./app/pages/Workflow";
import { Started, Stuck, Paused, Finished } from "./app/pages/ExecutionLogs";
import Execution from "./app/pages/Execution";
import UserBar from "./app/filter/UserBar";
import type { Statistics } from "./datamodel";

import reduce from "lodash/reduce";

type Props = {
  page: Page,
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
      page,
      environment,
      projectName,
      workflow,
      isLoading,
      closeUserbar,
      statistics
    } = this.props;

    if (isLoading) {
      return (
        <div className={classes.loading}>
          <Spinner />
        </div>
      );
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

      const renderTab = () => {
        switch (page.id) {
          case "workflow":
            return <Workflow workflow={workflow} job={page.jobId} />;
          case "executions/started":
            return <Started />;
          case "executions/stuck":
            return <Stuck />;
          case "executions/paused":
            return <Paused />;
          case "executions/finished":
            return <Finished />;
          case "executions/detail":
            return <Execution execution={page.execution} />;
          case "timeseries/calendar":
            return (
              <div style={{ padding: "1em" }}>
                <Link href="/executions/0c31e0c7-6401-4d9e-b34e-c22a553961bd">
                  random execution
                </Link>
              </div>
            );
          default:
            return null;
        }
      };

      return (
        <div className={classes.main} onClick={closeUserbar}>
          <section className={classes.leftpane}>
            <MenuHeader environment={environment} projectName={projectName} />
            <Menu active={page} statistics={statistics} />
          </section>
          <section className={classes.rightpane}>
            <UserBar
              className={classes.userBar}
              allTags={allTags}
              allJobs={allJobs}
            />
            {renderTab()}
          </section>
        </div>
      );
    }
  }
}

let styles = {
  leftpane: {
    width: "300px",
    minWidth: "300px",
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
    fontFamily: "Arial",
    height: "100vh"
  },
  userBar: {
    zIndex: 2
  },
  main: {
    backgroundColor: "#ECF1F5",
    display: "flex",
    alignItems: "stretch"
  },
  loading: {
    display: "flex",
    height: "100vh"
  }
};

const mapStateToProps = ({
  page,
  project,
  workflow,
  isLoading,
  statistics
}) => ({
  page,
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
