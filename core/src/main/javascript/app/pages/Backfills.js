// @flow

import React from "react";
import injectSheet from "react-jss";
import Link from "../components/Link";
import PopoverMenu from "../components/PopoverMenu";

type Props = {
  classes: any,
  selectedJobs: Array<string>,
  drillDown: (date: any) => void
};

class Backfills extends React.Component {
  props: Props;

  static defaultProps = {
    selectedJobs: []
  };

  constructor(props: Props) {
    super(props);
  }

  createBackfill() {}

  render() {
    let { classes, selectedJobs } = this.props;
    return (
      <div className={classes.container}>
        <h1 className={classes.title}>Backfills</h1>
        <PopoverMenu
          className={classes.menu}
          items={[
            <Link href="/timeseries/backfills/create">Create backfill</Link>
          ]}
        />
        <div className={classes.grid}>
          <div className={classes.data}>
            <div className={classes.noData}>
              No running backfills
              {selectedJobs.length ? " (some may have been filtered)" : ""}
            </div>
          </div>
        </div>
      </div>
    );
  }
}

// TODO duplicated styles
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
  },
  grid: {
    flex: "1",
    display: "flex",
    flexDirection: "column"
  },
  data: {
    display: "flex",
    flex: "1"
  },
  noData: {
    flex: "1",
    textAlign: "center",
    fontSize: "0.9em",
    color: "#8089a2",
    alignSelf: "center",
    paddingBottom: "15%"
  }
};
export default injectSheet(styles)(Backfills);
