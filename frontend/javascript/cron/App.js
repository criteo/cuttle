// @flow

import React from "react";
import classNames from "classnames";
import { connect } from "react-redux";
import injectSheet from "react-jss";
import FilterIcon from "react-icons/lib/md/search";

import MenuHeader from "../common/menu/MenuHeader";
import Spinner from "../common/components/Spinner";
import Menu from "./Menu";
import JobSelector from "./components/JobSelector";
import type { Page } from "./ApplicationState";
import * as Actions from "./actions";
import { Started, Finished, Retrying } from "./pages/ExecutionLogs";
import Execution from "./pages/Execution";
import Favicon from "./components/Favicon";
import type { Statistics, Workflow } from "./datamodel";
import { Dags } from "./pages/Dags";

type Props = {
  page: Page,
  projectName: string,
  projectVersion: ?string,
  env: {
    name: ?string,
    critical: boolean
  },
  statistics: Statistics,
  workflow: Workflow,
  isLoading: boolean,
  classes: any,
  selectedJobs: Array<string>,
  selectJobs: (dags: Array<string>) => void
};

class App extends React.Component<Props> {
  constructor(props: Props) {
    super(props);
  }

  render() {
    const {
      classes,
      page,
      env,
      projectName,
      projectVersion,
      workflow,
      isLoading,
      statistics,
      selectedJobs,
      selectJobs
    } = this.props;

    if (isLoading) {
      return (
        <div className={classes.loading}>
          <Spinner />
        </div>
      );
    } else {
      const renderTab = () => {
        switch (page.id) {
          case "executions/started":
            return <Started />;
          case "executions/finished":
            return <Finished />;
          case "executions/retrying":
            return <Retrying />;
          case "executions/detail":
            return <Execution execution={page.execution} />;
          case "dags":
            return <Dags status={page.status} />;
          default:
            return null;
        }
      };

      return (
        <div
          className={classNames(classes.main, {
            [classes.connectionLost]: statistics.error
          })}
        >
          <Favicon statistics={statistics} />
          <section className={classes.leftpane}>
            <MenuHeader
              env={env}
              projectName={projectName}
              projectVersion={projectVersion}
            />
            <Menu active={page} statistics={statistics} />
          </section>
          <section className={classes.rightpane}>
            <div className={classes.mainFilter}>
              <JobSelector
                workflow={workflow}
                selected={selectedJobs}
                placeholder={
                  <span>
                    <FilterIcon className={classes.filterIcon} />
                    Filter on specific jobs...
                  </span>
                }
                onChange={selectJobs}
              />
            </div>
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
    display: "flex",
    flexDirection: "column",
    backgroundColor: "#2F3647",
    color: "#758390",
    fontFamily: "Arial",
    height: "100vh",
    zIndex: "100"
  },
  rightpane: {
    display: "flex",
    flexGrow: "1",
    alignItems: "stretch",
    flexDirection: "column",
    backgroundColor: "#ECF1F5",
    color: "#3D4454",
    fontFamily: "Arial",
    height: "100vh",
    width: "calc(100vw - 300px)"
  },
  mainFilter: {
    zIndex: "2",
    background: "#fff",
    height: "4em",
    lineHeight: "4em",
    boxShadow: "0px 1px 5px 0px #BECBD6"
  },
  filterIcon: {
    transform: "scale(1.3) translateY(-1.5px) translateX(1px)",
    marginRight: "10px"
  },
  main: {
    backgroundColor: "#ECF1F5",
    display: "flex",
    alignItems: "stretch"
  },
  loading: {
    display: "flex",
    height: "100vh"
  },
  connectionLost: {
    filter: "grayscale(100%)"
  }
};

const mapStateToProps = ({
  app: { page, project, workflow, isLoading, statistics, selectedJobs }
}) => ({
  page,
  projectName: project && project.name,
  projectVersion: project && project.version,
  env: project && project.env,
  workflow,
  isLoading,
  statistics,
  selectedJobs
});

const mapDispatchToProps = dispatch => ({
  selectJobs(jobs: Array<string>) {
    dispatch(Actions.selectJobs(jobs));
  }
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(injectSheet(styles)(App));
