// @flow

import React from "react";
import ReactDOM from "react-dom";
import { connect } from "react-redux";
import classNames from "classnames";
import injectSheet from "react-jss";
import { goBack } from "redux-url";
import CloseIcon from "react-icons/lib/md/close";
import FullscreenIcon from "react-icons/lib/md/fullscreen";
import ExitFullscreenIcon from "react-icons/lib/md/fullscreen-exit";
import AutoScrollIcon from "react-icons/lib/md/arrow-downward";
import moment from "moment";

import Window from "../components/Window";
import FancyTable from "../components/FancyTable";
import Error from "../components/Error";
import Spinner from "../components/Spinner";
import Link from "../components/Link";
import JobStatus from "../components/JobStatus";
import { listenEvents } from "../../Utils";
import type { ExecutionLog } from "../../datamodel";

type Props = {
  classes: any,
  job: string,
  start: string,
  end: string,
  back: () => void
};

type State = {
  query: ?string,
  data: ?ExecutionLog,
  eventSource: ?any
};

class TimeseriesExecutions extends React.Component {
  props: Props;
  state: State;

  constructor(props: Props) {
    super(props);
    this.state = {
      query: null,
      data: null,
      eventSource: null
    };
  }

  render() {
    let { classes, job, start, end, back } = this.props;
    let { data } = this.state;

    return (
      <Window title="Executions for the period">
        <CloseIcon className={classes.close} onClick={back} />
      </Window>
    );
  }
}

const styles = {
  close: {
    position: "absolute",
    color: "#eef5fb",
    top: ".75em",
    right: ".5em",
    cursor: "pointer",
    fontSize: "20px"
  }
};

const mapStateToProps = ({}) => ({});
const mapDispatchToProps = dispatch => ({
  back() {
    dispatch(goBack());
  }
});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(TimeseriesExecutions)
);
