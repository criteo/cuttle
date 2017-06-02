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
import _ from "lodash";
import moment from "moment";

import Error from "../components/Error";
import Spinner from "../components/Spinner";
import Clock from "../components/Clock";
import Link from "../components/Link";
import JobStatus from "../components/JobStatus";
import { listenEvents } from "../../Utils";
import type { ExecutionLog, Workflow } from "../../datamodel";

type Line = {
  timestamp: string,
  level: "DEBUG" | "INFO" | "ERROR",
  message: string
};

type Props = {
  classes: any,
  execution: string,
  back: () => void
};

type State = {
  query: ?string,
  data: ?ExecutionLog,
  streams: Array<Line>,
  eventSource: ?any,
  streamsEventSource: ?any,
  error: ?any,
  fullscreen: boolean,
  autoScroll: boolean
};

class Execution extends React.Component {
  props: Props;
  state: State;
  scroller: ?any;

  constructor(props: Props) {
    super(props);
    this.state = {
      query: null,
      data: null,
      streams: [],
      eventSource: null,
      streamsEventSource: null,
      error: null,
      fullscreen: false,
      autoScroll: false
    };
  }

  listen() {
    let { query, eventSource, streamsEventSource } = this.state;
    let { execution } = this.props;
    let newQuery = `/api/executions/${execution}?events=true`;
    if (newQuery != query) {
      eventSource && eventSource.close();
      streamsEventSource && streamsEventSource.close();
      eventSource = listenEvents(
        newQuery,
        this.updateData.bind(this),
        this.notFound.bind(this)
      );
      streamsEventSource = listenEvents(
        `/api/executions/${execution}/streams?events=true`,
        this.streams.bind(this)
      );
      this.setState({
        ...this.state,
        query: newQuery,
        data: null,
        streams: [],
        eventSource,
        streamsEventSource,
        fullscreen: false,
        autoScroll: false
      });
    }
  }

  notFound(error) {
    this.setState({
      ...this.state,
      error
    });
  }

  updateData(json: ExecutionLog) {
    this.setState({
      ...this.state,
      data: json,
      autoScroll: this.state.autoScroll || json.status == "running"
    });
  }

  streams(json: any) {
    let { streamsEventSource } = this.state;
    if (json == "EOS" && streamsEventSource) {
      streamsEventSource.close();
    } else {
      let lines = [];
      json.forEach(line => {
        let groups = /([^ ]+) ([^ ]+)\s+- (.*)/.exec(line);
        if (groups) {
          let [_, timestamp, level, message] = groups;
          lines.push({
            timestamp: moment(timestamp).format("YYYY-MM-DD HH:mm:ss"),
            level,
            message
          });
        }
      });
      this.setState({
        ...this.state,
        streams: this.state.streams.concat(lines)
      });
    }
  }

  componentDidUpdate() {
    this.listen();
    if (this.scroller && this.state.autoScroll) {
      this.scroller.scrollTop = this.scroller.scrollHeight;
    }
  }

  componentWillMount() {
    this.listen();
  }

  componentWillUnmount() {
    let { eventSource, streamsEventSource } = this.state;
    eventSource && eventSource.close();
    streamsEventSource && streamsEventSource.close();
  }

  storeStreamRef(ref) {
    this.scroller = ReactDOM.findDOMNode(ref);
  }

  onClickFullscreen(fullscreen: boolean) {
    this.setState({
      ...this.state,
      fullscreen
    });
  }

  onClickAutoScroll(autoScroll: boolean) {
    this.setState({
      ...this.state,
      autoScroll
    });
  }

  detectManualScroll() {
    if (this.scroller) {
      if (
        this.scroller.scrollHeight - this.scroller.offsetHeight !=
        this.scroller.scrollTop
      ) {
        this.setState({
          ...this.state,
          autoScroll: false
        });
      }
    }
  }

  render() {
    let { classes, execution, back } = this.props;
    let { data, error, streams } = this.state;

    return (
      <div className={classes.container}>
        <div className={classes.overlay}>
          <h1 className={classes.title}>Execution detail</h1>
          <CloseIcon className={classes.close} onClick={back} />
          {data
            ? [
                <dl className={classes.definitions} key="properties">
                  <dt key="id">Id:</dt>
                  <dd key="id_">{data.id}</dd>
                  <dt key="job">Job:</dt>
                  <dd key="job_">{data.job}</dd>
                  <dt key="status">Status:</dt>
                  <dd key="status_"><JobStatus status={data.status} /></dd>
                  {data.startTime
                    ? [
                        <dt key="startTime">Start time:</dt>,
                        <dd key="startTime_">
                          {moment(data.startTime)
                            .utc()
                            .format("dddd, MMMM Do YYYY, hh:mm:ss z")}
                        </dd>
                      ]
                    : null}
                  {data.endTime
                    ? [
                        <dt key="endTime">End time:</dt>,
                        <dd key="endTime_">
                          {moment(data.endTime)
                            .utc()
                            .format("dddd, MMMM Do YYYY, hh:mm:ss z")}
                        </dd>
                      ]
                    : null}
                  {data.startTime
                    ? [
                        <dt key="duration">Duration:</dt>,
                        <dd key="duration_">
                          {data.endTime
                            ? moment
                                .utc(
                                  moment(data.endTime).diff(
                                    moment(data.startTime)
                                  )
                                )
                                .format("HH:mm:ss")
                            : <Clock time={data.startTime} humanize={false} />}
                        </dd>
                      ]
                    : null}
                  {data.failing
                    ? [
                        <dt key="failing">Failing:</dt>,
                        <dd key="failing_">
                          {`Failed ${data.failing.failedExecutions.length} times and will be retried`}
                          {" "}
                          <Clock time={data.failing.nextRetry || ""} />
                          .&nbsp;
                          <Link
                            className={classes.failedLink}
                            href={`/executions/${(data.failing: any).failedExecutions[(data.failing: any).failedExecutions.length - 1].id}`}
                          >
                            Check latest failed execution.
                          </Link>
                        </dd>
                      ]
                    : null}
                </dl>,
                <div
                  className={classNames(classes.streams, {
                    [classes.fullscreen]: this.state.fullscreen
                  })}
                  key="streams"
                >
                  <ul
                    ref={this.storeStreamRef.bind(this)}
                    onScroll={this.detectManualScroll.bind(this)}
                  >
                    {streams.map(({ timestamp, level, message }, i) => {
                      return (
                        <li key={i}>
                          <span>{timestamp}</span>
                          <p className={classes[level]}>{message}</p>
                        </li>
                      );
                    })}
                  </ul>
                  {this.state.fullscreen
                    ? <ExitFullscreenIcon
                        onClick={this.onClickFullscreen.bind(this, false)}
                        className={classes.fullscreenButton}
                      />
                    : <FullscreenIcon
                        onClick={this.onClickFullscreen.bind(this, true)}
                        className={classes.fullscreenButton}
                      />}
                  <AutoScrollIcon
                    onClick={this.onClickAutoScroll.bind(
                      this,
                      !this.state.autoScroll
                    )}
                    className={classNames(classes.autoScrollButton, {
                      [classes.activeAutoScroll]: this.state.autoScroll
                    })}
                  />
                </div>
              ]
            : error
                ? <Error message={`execution ${execution} not found`} />
                : <Spinner />}
        </div>
      </div>
    );
  }
}

const styles = {
  container: {
    padding: "1em",
    flex: "1",
    display: "flex",
    flexDirection: "column",
    position: "relative"
  },
  overlay: {
    background: "#ffffff",
    flex: "1",
    position: "relative",
    padding: "1em",
    boxShadow: "0px 0px 15px 0px #799cb7",
    borderRadius: "2px",
    overflow: "hidden",
    display: "flex",
    flexDirection: "column"
  },
  title: {
    fontSize: "1em",
    margin: "-1em -1em 1em -1em",
    padding: "1em",
    color: "#f9fbfc",
    background: "#5c6477",
    fontWeight: "normal",
    boxShadow: "0px 0px 15px 0px #799cb7"
  },
  close: {
    position: "absolute",
    color: "#eef5fb",
    top: ".75em",
    right: ".5em",
    cursor: "pointer",
    fontSize: "20px"
  },
  definitions: {
    margin: "-1em",
    display: "flex",
    flexFlow: "row",
    flexWrap: "wrap",
    fontSize: ".85em",
    background: "rgba(189, 213, 228, 0.1)",
    "& dt": {
      flex: "0 0 150px",
      textOverflow: "ellipsis",
      overflow: "hidden",
      padding: "0 1em",
      boxSizing: "border-box",
      textAlign: "right",
      color: "#637686",
      lineHeight: "2.75em"
    },
    "& dd": {
      flex: "0 0 calc(100% - 150px)",
      marginLeft: "auto",
      textAlign: "left",
      textOverflow: "ellipsis",
      overflow: "hidden",
      padding: "0",
      boxSizing: "border-box",
      lineHeight: "2.75em"
    },
    "& dd:nth-of-type(even), & dt:nth-of-type(even)": {
      background: "#eef5fb"
    }
  },
  failedLink: {
    color: "#e91e63"
  },
  streams: {
    flex: "1",
    display: "flex",
    background: "#23252f",
    margin: "1em -1em -1em -1em",
    position: "relative",

    "& ul": {
      flex: "1",
      overflow: "scroll",
      padding: "1em",
      margin: "0",
      listStyle: "none",
      fontSize: ".85em",
      lineHeight: "1.5em"
    },

    "& span": {
      width: "150px",
      color: "#747a88",
      display: "inline-block",
      marginRight: "-10px",
      boxSizing: "border-box"
    },

    "& p": {
      display: "inline-block",
      margin: "0",
      color: "#f1f1f1",
      whiteSpace: "pre"
    }
  },
  fullscreen: {
    position: "fixed",
    top: "0",
    left: "0",
    right: "0",
    bottom: "0",
    margin: "0",
    zIndex: "99999"
  },
  fullscreenButton: {
    cursor: "pointer",
    color: "#fff",
    fontSize: "22px",
    position: "absolute",
    right: "10px",
    top: "10px"
  },
  autoScrollButton: {
    cursor: "pointer",
    color: "#fff",
    fontSize: "22px",
    position: "absolute",
    right: "36px",
    top: "10px"
  },
  activeAutoScroll: {
    color: "#66cb63"
  },
  DEBUG: {
    color: "#FFFF91 !important"
  },
  ERROR: {
    color: "#FF6C60 !important"
  }
};

const mapStateToProps = ({}) => ({});
const mapDispatchToProps = dispatch => ({
  back() {
    dispatch(goBack());
  }
});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(Execution)
);
