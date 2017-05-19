// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";

import PopoverMenu from "../components/PopoverMenu";
import type { Workflow } from "../../datamodel";
import ExecutionLogs from "../components/ExecutionLogs";

type Props = {
  classes: any,
  workflow: Workflow
};

const Started = ({ classes, workflow }: Props) => {
  let pauseAll = () => fetch("/api/jobs/all/pause", {method: "POST"});
  return (
    <div className={classes.container}>
      <h1 className={classes.title}>Started executions</h1>
      <PopoverMenu
        className={classes.menu}
        items={[<span onClick={pauseAll}>Pause everything</span>]}
      />
      <ExecutionLogs
        workflow={workflow}
        columns={["job", "context", "startTime", "status", "detail"]}
        request={(page, rowsPerPage, sort) =>
          `/api/executions/started?stream=true&offset=${page * rowsPerPage}&limit=${rowsPerPage}&sort=${sort.column}&order=${sort.order}`}
        label="started"
        defaultSort={{ column: "context", order: "asc" }}
      />
    </div>
  );
};

const styles = {
  container: {
    padding: "1em",
    flex: "1",
    display: "flex",
    flexDirection: "column",
    position: "relative"
  },
  title: {
    fontSize: "1.2em",
    margin: "0 0 16px 0",
    color: "#607e96",
    fontWeight: "normal"
  },
  menu: {
    position: "absolute",
    top: "1em",
    right: "1em"
  }
};

const mapStateToProps = ({ workflow }) => ({ workflow });

export default connect(mapStateToProps)(
  injectSheet(styles)(Started)
);
