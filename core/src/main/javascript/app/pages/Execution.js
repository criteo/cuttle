// @flow

import React from "react";
import injectSheet from "react-jss";
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
import StreamView from "../components/StreamView";
import TextWithDashedLine from "../components/TextWithDashedLine";

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
  error: ?any
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

const Menu = ({
  executionLog,
  classes
}: {
  executionLog: ExecutionLog,
  classes: Object
}) => {
  const isCancellable =
    executionLog.status === "running" || executionLog.status === "waiting";

  return isCancellable
    ? <PopoverMenu
        keys="menu"
        className={classes.menu}
        items={[
          <span
            onClick={() => {
              fetch(`/api/executions/${executionLog.id}/cancel`, {
                method: "POST",
                credentials: "include"
              });
            }}
          >
            Cancel
          </span>
        ]}
      />
    : null;
};

class Execution extends React.Component {
  props: Props;
  state: State;
  shouldOverwriteStreams: boolean = false;

  constructor(props: Props) {
    super(props);

    this.state = {
      query: null,
      data: null,
      streams: [],
      eventSource: null,
      streamsEventSource: null,
      error: null
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
        streamsEventSource
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
  }

  componentWillMount() {
    this.listen();
  }

  componentWillUnmount() {
    let { eventSource, streamsEventSource } = this.state;
    eventSource && eventSource.close();
    streamsEventSource && streamsEventSource.close();
  }

  render() {
    let { classes, execution } = this.props;
    let { data, error, streams } = this.state;

    const isExecutionWaiting =
      !data ||
      data.status === "waiting" ||
      data.status === "throttled" ||
      data.status === "paused";

    return (
      <Window title="Execution">
        {data
          ? [
              <Menu key="menu" executionLog={data} classes={classes} />,
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
                          .format("dddd, MMMM Do YYYY, HH:mm:ss z")}
                      </dd>
                    ]
                  : null}
                {data.endTime
                  ? [
                      <dt key="endTime">End time:</dt>,
                      <dd key="endTime_">
                        {moment(data.endTime)
                          .utc()
                          .format("dddd, MMMM Do YYYY, HH:mm:ss z")}
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
                                key="progressBar"
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
              <StreamView
                key="streams"
                streams={streams}
                placeholder={
                  isExecutionWaiting
                    ? <li key="waiting">
                        <TextWithDashedLine text={"Execution is waiting"} />
                      </li>
                    : null
                }
              />
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
  }
};

export default injectSheet(styles)(Execution);
