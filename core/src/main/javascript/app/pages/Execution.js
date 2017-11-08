// @flow

import React from "react";
import classNames from "classnames";
import injectSheet from "react-jss";
import FullscreenIcon from "react-icons/lib/md/fullscreen";
import ExitFullscreenIcon from "react-icons/lib/md/fullscreen-exit";
import AutoScrollIcon from "react-icons/lib/md/arrow-downward";
import ReactTooltip from "react-tooltip";
import PopoverMenu from "../components/PopoverMenu";
import moment from "moment";

import Context from "../components/Context";
import Window from "../components/Window";
import FancyTable from "../components/FancyTable";
import Error from "../components/Error";
import Spinner from "../components/Spinner";
import Clock from "../components/Clock";
import Link from "../components/Link";
import Status from "../components/Status";
import { Badge } from "../components/Badge";
import { listenEvents } from "../../Utils";
import type { ExecutionLog } from "../../datamodel";
import { highlightURLs } from "../utils/URLHighlighter";

type Line = {
  timestamp: string,
  level: "DEBUG" | "INFO" | "ERROR",
  message: string
};

type Props = {
  classes: any,
  execution: string
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

type ProgressBarProps = {
  classes: any,
  totalTimeSeconds: number,
  waitingTimeSeconds: number
};

const ProgressBarComponent = ({
  classes,
  totalTimeSeconds,
  waitingTimeSeconds
}: ProgressBarProps) => {
  const totalWidth = 200;
  const height = 8;
  const barWidth = totalTimeSeconds !== 0
    ? waitingTimeSeconds / totalTimeSeconds * totalWidth
    : 0;

  const tooltip =
    `Waiting : ${moment.utc(waitingTimeSeconds * 1000).format("HH:mm:ss")} / ` +
    `Running : ${moment
      .utc((totalTimeSeconds - waitingTimeSeconds) * 1000)
      .format("HH:mm:ss")}`;

  return (
    <span className={classes.main}>
      <svg width={totalWidth} height={height} className={classes.svg}>
        <g data-tip={tooltip}>
          <rect width={totalWidth} height={height} fill="#00BCD4" />
          <rect width={barWidth} height={height} fill="#ff9800" />
        </g>
      </svg>
      <ReactTooltip className={classes.tooltip} effect="float" html={true} />
    </span>
  );
};

const ProgressBar = injectSheet({
  main: {
    "margin-left": "10px"
  },
  svg: {
    lineHeight: "18px",
    display: "inline-block"
  }
})(ProgressBarComponent);

class Execution extends React.Component {
  props: Props;
  state: State;
  scroller: ?any;
  shouldOverwriteStreams: boolean = false;

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
        `/api/executions/${execution}/streams`,
        this.streams.bind(this)
      );
      this.setState({
        query: newQuery,
        data: null,
        streams: [],
        eventSource,
        streamsEventSource,
        fullscreen: false,
        autoScroll: true
      });
    }
  }

  notFound(error) {
    this.setState({
      error
    });
  }

  updateData(json: ExecutionLog) {
    this.setState({
      ...this.state,
      data: json
    });
  }

  streams(json: any) {
    let { streamsEventSource } = this.state;
    if (json == "EOS" && streamsEventSource) {
      streamsEventSource.close();
    } else if (json == "BOS" && streamsEventSource) {
      this.shouldOverwriteStreams = true;
    } else {
      let lines = [];
      json.map(line => {
        let groups = /([^ ]+) ([^ ]+)\s+- (.*)/.exec(line);
        if (groups) {
          let [_, timestamp, level, message] = groups;
          lines.push({
            timestamp: moment.utc(timestamp).format("YYYY-MM-DD HH:mm:ss"),
            level,
            message
          });
        }
      });
      const streamsHead = this.shouldOverwriteStreams ? [] : this.state.streams;
      this.shouldOverwriteStreams = false;
      this.setState({
        streams: streamsHead.concat(lines)
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

  onClickFullscreen(fullscreen: boolean) {
    this.setState({
      fullscreen
    });
  }

  onClickAutoScroll(autoScroll: boolean) {
    this.setState({
      autoScroll
    });
  }

  detectManualScroll() {
    const manualScroll =
      this.scroller &&
      this.scroller.scrollHeight - this.scroller.offsetHeight !=
        this.scroller.scrollTop;

    if (manualScroll)
      this.setState({
        autoScroll: false
      });
  }

  render() {
    let { classes, execution } = this.props;
    let { data, error, streams } = this.state;

    const menu = (e: ExecutionLog) => {
      const isCancellable = e.status == "running" || e.status == "waiting";

      if (isCancellable) {
        return (
          <PopoverMenu
            className={classes.menu}
            items={[
              <span
                onClick={() => {
                  fetch(`/api/executions/${e.id}/cancel`, {
                    method: "POST",
                    credentials: "include"
                  });
                }}
              >
                Cancel
              </span>
            ]}
          />
        );
      }
      return null;
    };

    return (
      <Window title="Execution">
        {data
          ? [
              menu(data),
              <FancyTable key="properties">
                <dt key="id">Id:</dt>
                <dd key="id_">{data.id}</dd>
                <dt key="job">Job:</dt>
                <dd key="job_">
                  <Link href={`/workflow/${data.job}`}>{data.job}</Link>
                </dd>
                <dt key="context">Context:</dt>
                <dd key="context_"><Context context={data.context} /></dd>
                <dt key="status">Status:</dt>
                <dd key="status_">
                  <Status status={data.status} />{data.context.backfill
                    ? <span style={{ marginLeft: "10px" }}>
                        <Link
                          href={`/timeseries/backfills/${data.context.backfill.id}`}
                        >
                          <Badge label="BACKFILL" kind="alt" />
                        </Link>
                      </span>
                    : null}
                </dd>
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
                          ? [
                              moment
                                .utc(
                                  moment(data.endTime).diff(
                                    moment(data.startTime)
                                  )
                                )
                                .format("HH:mm:ss"),
                              <ProgressBar
                                totalTimeSeconds={
                                  moment(data.endTime).diff(
                                    moment(data.startTime)
                                  ) / 1000
                                }
                                waitingTimeSeconds={data.waitingSeconds}
                              />
                            ]
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
              </FancyTable>,
              <div
                className={classNames(classes.streams, {
                  [classes.fullscreen]: this.state.fullscreen
                })}
                key="streams"
              >
                <ul
                  ref={r => (this.scroller = r)}
                  onScroll={this.detectManualScroll.bind(this)}
                >
                  {streams.map(({ timestamp, level, message }, i) => {
                    return (
                      <li key={i}>
                        <span>{timestamp}</span>
                        <p className={classes[level]}>
                          {highlightURLs(message)}
                        </p>
                      </li>
                    );
                  })}
                  {data.status == "waiting" ||
                    data.status == "throttled" ||
                    data.status == "paused"
                    ? <li key="waiting" className={classes.waiting}>
                        <svg
                          width="100%"
                          height="2"
                          version="1.1"
                          xmlns="http://www.w3.org/2000/svg"
                        >
                          <line x1="-10" y1="0" x2="100%" y2="0" />
                        </svg>
                        <span>Execution is waiting</span>
                      </li>
                    : null}
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
      </Window>
    );
  }
}

const styles = {
  menu: {
    position: "absolute",
    top: "60px",
    right: "1em"
  },
  failedLink: {
    color: "#e91e63"
  },
  streams: {
    flex: "1",
    display: "flex",
    background: "#23252f",
    position: "relative",

    "& ul": {
      flex: "1",
      overflow: "scroll",
      padding: "1em",
      margin: "0",
      listStyle: "none",
      fontSize: "12px",
      fontFamily: "Fira Mono",
      lineHeight: "1.6em",
      whiteSpace: "nowrap"
    },

    "& span": {
      color: "#747a88",
      display: "inline-block",
      marginRight: "15px",
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
    right: "20px",
    top: "10px",
    background: "rgba(35, 37, 47, 0.65)"
  },
  autoScrollButton: {
    cursor: "pointer",
    color: "#fff",
    fontSize: "22px",
    position: "absolute",
    right: "46px",
    top: "10px",
    background: "rgba(35, 37, 47, 0.65)"
  },
  activeAutoScroll: {
    color: "#66cb63"
  },
  DEBUG: {
    color: "#FFFF91 !important"
  },
  ERROR: {
    color: "#FF6C60 !important"
  },
  waiting: {
    position: "relative",
    margin: "5px 0",
    "& line": {
      stroke: "#FFFF91",
      strokeWidth: "2px",
      strokeDasharray: "5,5",
      animation: "dashed 500ms linear infinite"
    },
    "& span": {
      color: "#FFFF91",
      position: "absolute",
      textAlign: "center",
      left: "50%",
      top: "2px",
      display: "inline-block",
      width: "180px",
      marginLeft: "-90px",
      background: "#23252f"
    }
  }
};

export default injectSheet(styles)(Execution);
