// @flow

import React from "react";
import { connect } from "react-redux";
import classNames from "classnames";
import { navigate } from "redux-url";
import injectSheet from "react-jss";
import moment from "moment";
import _ from "lodash";
import * as d3 from "d3";

import ChevronIcon from "react-icons/lib/md/chevron-right";
import ToIcon from "react-icons/lib/md/arrow-forward";
import ReactTooltip from "react-tooltip";

import ZoomIn from "react-icons/lib/md/zoom-in";
import ZoomOut from "react-icons/lib/md/zoom-out";
import ArrowNext from "react-icons/lib/md/arrow-forward";
import ArrowPrevious from "react-icons/lib/md/arrow-back";

import Link from "../components/Link";
import Spinner from "../components/Spinner";

import type { Workflow } from "../../datamodel";

import { listenEvents } from "../../Utils";

type Props = {
  classes: any,
  workflow: Workflow,
  selectedJobs: Array<string>,
  start: string,
  end: string,
  drillDown: (from: any, to: any) => void,
  showExecutions: (job: string, from: any, to: any) => void
};

type Period = {
  start: string,
  end: string
};

type SummarySlot = {
  period: Period,
  completion: number,
  error: boolean,
  backfill: boolean,
  aggregated: true
};

type JobPeriodSlot =
  | {
      period: Period,
      status: "done" | "running" | "todo" | "paused",
      backfill: boolean,
      version: string,
      aggregated: false
    }
  | {
      period: Period,
      completion: string,
      error: boolean,
      backfill: boolean,
      aggregated: true
    };

type Stats = {
  summary: SummarySlot[],
  jobs: { [job: string]: JobPeriodSlot[] }
};

type State = {
  data: ?Stats,
  query: ?string,
  eventSource: ?any,
  showVersion: boolean
};

// Static/format helpers
let globalLabel = "Workflow";

let LEFT_MARGIN = 30;
let RIGHT_MARGIN = 50;
let ROW_HEIGHT = 20;
let PADDING = 5;
let MARGIN = 1;
let VERSION_RECT_SIZE = 0.4 * ROW_HEIGHT;
let LEGEND_WIDTH = 80;

let versionPointer = 0;
const versionColorMap = new Map();
const versionsColor = [
  "#ff9900",
  "#00e6e6",
  "#7a00cc",
  "#996600",
  "#e6e600",
  "#0000cc",
  "#b300b3"
];

let formatDate = (date: string) =>
  moment(date)
    .utc()
    .format("YYYY-MM-DD HH:mm");

let colorScale = d3.interpolateRgb("#ecf1f5", "#62cc64");

let tickFormat = date =>
  (d3.utcHour(date) < date
    ? d3.utcFormat("")
    : d3.utcDay(date) < date
      ? d3.utcFormat("%H:00")
      : d3.utcFormat("%Y-%m-%d"))(date);

let getMaxLabelWidth = (jobNames, svg, jobNameClass) => {
  let g = svg.append("g").attr("id", "widthHack");
  _.forEach(jobNames, job => {
    g
      .append("text")
      .attr("class", jobNameClass)
      .text(job);
  });
  let labelWidth = g.node().getBBox().width + LEFT_MARGIN;
  svg.select("g#widthHack").remove();
  return labelWidth;
};

let getLabelForStatus = status => {
  switch(status) {
    case "failed": return "stuck"
    case "successful": return "done"
    case "running": return "started"
    case "paused": return "paused"
    default: return "todo"
  }
};

// Drawing methods // Summary periods
const summaryPeriodHelper = (x1, x2) => ({
  tip: ({ period, completion }) =>
    `<div>${Math.ceil(completion * 100)}% complete – ${formatDate(
      period.start
    )} to ${formatDate(period.end)} UTC</div>`,
  fill: ({ error, completion }) =>
    error ? "#e91e63" : completion == 1 ? "#62cc64" : "#ecf1f5"
});

// drawing methods // job details
const jobPeriodsHelper = (x1, x2, showExecutions, drillDown) => ({
  tip: ({ period, status, jobName }) =>
    `<div>${jobName} is ${getLabelForStatus(status)} – ${formatDate(period.start)} to ${formatDate(period.end)} UTC</div>`,
  translate: ({ period }) => `translate(${x1(period) + MARGIN}, 0)`,
  width: ({ period }) => x2(period) - x1(period),
  fill: ({ status }) =>
    status == "failed"
      ? "#e91e63"
      : status == "successful"
        ? "#62cc64"
        : status == "waiting"
          ? "#ffbc5a"
          : status == "running"
            ? "#49d3e4"
              : status == "paused"
                ? "#ffaaff"
                : "#ecf1f5",
  // For aggregated periods, we want to zoom on click
  click: ({ period, jobId, aggregated }) =>
    aggregated
      ? drillDown(moment.utc(period.start), moment.utc(period.end))
      : showExecutions(jobId, moment.utc(period.start), moment.utc(period.end)),
  // For aggregated periods, we want to zoom on click
  cursor: ({ aggregated }) => (aggregated ? "zoom-in" : "pointer"),
  // No stroke (thick border) for simple periods
  stroke: ({ error, completion = 0 }) =>
    error ? "#e91e63" : colorScale(completion)
});

// convert a version string to a specific color
const stringToColor = function(str) {
  // if this version was already associated with a color, return it
  if (versionColorMap.has(str)) {
    return versionColorMap.get(str);
  } else {
    // chose the next color available in the list
    let newColor = versionsColor[versionPointer % (versionsColor.length - 1)];
    versionColorMap.set(str, newColor);
    versionPointer++;
    return newColor;
  }
};

// method drawing all the cases of color for job periods (for specific jobs in the calendar focus)
// A summary period is an aggregated period with a special tooltip (not dedicated to a job)
const drawJobPeriods = (
  enterPeriodNode,
  periodClass,
  x1,
  x2,
  showExecutions,
  showVersion,
  drillDown,
  summary = false
) => {
  const helper = jobPeriodsHelper(x1, x2, showExecutions, drillDown);
  const aggregatedHelper = summaryPeriodHelper(x1, x2);
  const adjustedWidth = ({ period }) => helper.width({ period }) - 2 * MARGIN;
  const strokeWidth = 4;
  const xyOffset = ({ aggregated }) =>
    MARGIN + (aggregated || summary ? strokeWidth / 2 : 0);
  const cursor = summary ? "default" : helper.cursor;
  const newPeriodSlot = enterPeriodNode
    .append("g")
    .attr("class", classNames("periodSlot", periodClass))
    .attr(
      "data-tip",
      d => (d.aggregated || summary ? aggregatedHelper.tip(d) : helper.tip(d))
    )
    .attr("transform", helper.translate);

  // Summmary periods are not clickable
  if (!summary) newPeriodSlot.on("click", helper.click);

  // Transparent rectangle behind
  newPeriodSlot
    .append("rect")
    .attr("class", "placeholder")
    .attr("height", ROW_HEIGHT + 2 * MARGIN)
    .attr("fill", "transparent")
    .attr("width", helper.width)
    .style("cursor", cursor);

  // The main indicator (for summary and aggregated periods, there is a green border, for simple periods,
  // we display multiple colors in relation with the status of the job for the given period)
  newPeriodSlot
    .append("rect")
    .attr("class", "colored")
    .attr("y", xyOffset)
    .attr("x", xyOffset)
    .attr(
      "height",
      ({ backfill, aggregated }) =>
        ROW_HEIGHT -
        (aggregated || summary ? strokeWidth : 0) -
        (backfill ? 6 : 0)
    )
    .attr(
      "width",
      d => adjustedWidth(d) - (d.aggregated || summary ? strokeWidth : 0)
    )
    .attr(
      "fill",
      d => (d.aggregated || summary ? aggregatedHelper.fill(d) : helper.fill(d))
    )
    .attr("stroke-width", d => (d.aggregated || summary ? strokeWidth : 0))
    .attr(
      "stroke",
      d => (d.aggregated || summary ? helper.stroke(d) : "transparent")
    )
    .style("cursor", cursor);

  // A purple line is added for periods of ab backfilled job
  newPeriodSlot
    .selectAll("rect.backfill")
    .data(d => (d.backfill ? [d] : []), k => k.period.start)
    .enter()
    .append("rect")
    .attr("class", "backfill")
    .attr("width", adjustedWidth)
    .attr("height", 3)
    .attr("fill", "#bb65ca")
    .attr("x", MARGIN)
    .attr("y", ROW_HEIGHT - 3);

  // If the version box is checked, draw a color rectangle for each done job
  newPeriodSlot
    .selectAll("rect.version")
    .data(d => (showVersion && d.version ? [d] : []), k => k.period.start)
    .enter()
    .append("rect")
    .attr("class", "version")
    .attr("width", VERSION_RECT_SIZE)
    .attr("height", VERSION_RECT_SIZE)
    .attr("fill", d => stringToColor(d.version))
    .style("opacity", 1.0)
    .attr("x", MARGIN)
    .attr("y", 0);
};

class CalendarFocus extends React.Component<Props, State> {
  vizContainer: any;
  summarySvgContainer: any;
  detailsSvgContainer: any;
  legendContainer: any;

  constructor(props: Props) {
    super(props);
    this.state = {
      data: null,
      query: null,
      eventSource: null,
      showVersion: false
    };
  }

  toggleShowVersion() {
    this.setState({
      showVersion: !this.state.showVersion
    });
  }

  listenForUpdates(props: Props) {
    let { start, end } = props;
    let jobsFilter =
      props.selectedJobs.length > 0
        ? `&jobs=${props.selectedJobs.join(",")}`
        : "";
    let query = `/api/timeseries/calendar/focus?start=${moment(
      start
    ).toISOString()}&end=${moment(end).toISOString()}${jobsFilter}`;
    let { query: currentQuery, eventSource: currentEventSource } = this.state;
    if (currentQuery != query) {
      currentEventSource && currentEventSource.close();
      let eventSource = listenEvents(query, this.updateData.bind(this));
      this.setState({
        data: null,
        eventSource
      });
    }
  }

  drawViz(props: Props, state: State) {
    let { data } = state;
    let { classes, start, end, workflow } = props;

    if (data && data.summary.length) {
      let { summary, jobs } = data;
      let width = this.vizContainer.clientWidth;

      let summarySvg = d3
        .select(this.summarySvgContainer)
        .attr("width", width)
        .attr("height", 80);

      // compute label sizes
      const jobsIds = _.keys(jobs);
      const getJobIndex = id => _.findIndex(workflow.jobs, j => j.id == id);
      const getJobName = id => workflow.jobs[getJobIndex(id)].name;
      const jobNames = _.flatMap(jobsIds, getJobName);
      let labelWidth = getMaxLabelWidth(
        [...jobNames, globalLabel],
        summarySvg,
        classes.jobName
      );

      // Time axis
      let axisWidth = width - labelWidth - RIGHT_MARGIN;
      let timeScale = d3
        .scaleUtc()
        .domain([moment(start).toDate(), moment(end).toDate()])
        .range([0, axisWidth]);

      let timeAxis = d3
        .axisTop(timeScale)
        .tickFormat(tickFormat)
        .ticks(5);
      summarySvg
        .select("g#axisContainer")
        .html(null)
        .attr("transform", `translate(${labelWidth}, ${ROW_HEIGHT + PADDING})`)
        .attr("class", classes.axis)
        .call(timeAxis);

      let x1 = period => timeScale(moment(period.start)),
        x2 = period => timeScale(moment(period.end));

      // Summary
      const summaryGroup = summarySvg
        .select("g#summary")
        .html(null)
        .attr("transform", `translate(${labelWidth}, 35)`);

      summaryGroup
        .append("text")
        .attr("class", classNames(classes.jobName))
        .attr("text-anchor", "end")
        .attr("x", -(PADDING * 2))
        .attr("y", 14)
        .text(globalLabel);

      summaryGroup
        .selectAll("g.periodSlot")
        .data(summary, k => k.period.start)
        .enter()
        .call(
          drawJobPeriods,
          classes.aggregatedPeriod,
          x1,
          x2,
          this.props.showExecutions,
          state.showVersion,
          this.props.drillDown,
          true
        );

      // Breakdown
      let detailsSvg = d3
        .select(this.detailsSvgContainer)
        .html(null)
        .attr("width", width)
        .attr("height", _.keys(jobs).length * (ROW_HEIGHT + 2 * MARGIN));

      // draw a timeline for each job selected
      let drawJobs = enterSelection => {
        let newJobTimeline = enterSelection
          .append("g")
          .attr("class", "jobTimeline")
          .attr(
            "transform",
            (d, i) =>
              `translate(${labelWidth}, ${i * (ROW_HEIGHT + 2 * MARGIN)})`
          );
        newJobTimeline
          .append("text")
          .attr("class", classes.jobName)
          .attr("text-anchor", "end")
          .attr("x", -(2 * PADDING))
          .attr("y", 14)
          .text(d => d.name);

        newJobTimeline
          .selectAll("g.periodSlot")
          .data(
            job =>
              _.map(jobs[job.id], p => ({
                ...p,
                jobName: job.name,
                jobId: job.id
              })),
            k => k.period.start
          )
          .enter()
          .call(
            drawJobPeriods,
            classes.period,
            x1,
            x2,
            this.props.showExecutions,
            state.showVersion,
            this.props.drillDown,
            false
          );
      };

      const jobInfos = _.filter(workflow.jobs, j =>
        _.includes(_.keys(jobs), j.id)
      );

      detailsSvg
        .selectAll("g.jobTimeline")
        .data(jobInfos, job => job.id)
        .enter()
        .call(drawJobs);

      // display legend for every distinct project version
      const versions = Object.keys(jobs).map(k =>
        jobs[k].map(
          periodSlot =>
            !periodSlot.aggregated ? periodSlot.version : undefined
        )
      );
      const distinctVersions = [...new Set([].concat(...versions))].filter(
        version => version !== "" && version !== undefined
      );

      const versionNodes = d3
        .select(this.legendContainer)
        .attr("width", axisWidth)
        .selectAll("g.jobVersionLegend")
        .data(state.showVersion ? distinctVersions : []);

      const versionGroup = versionNodes
        .enter()
        .append("g")
        .attr("width", LEGEND_WIDTH)
        .attr("height", 20)
        .attr("class", "jobVersionLegend")
        .attr("text-anchor", "end")
        .attr(
          "transform",
          (d, i) =>
            `translate(${axisWidth - LEGEND_WIDTH - labelWidth}, ${i *
              (ROW_HEIGHT + 2 * MARGIN)})`
        );

      versionGroup
        .append("rect")
        .attr("class", "legend")
        .attr("width", VERSION_RECT_SIZE)
        .attr("height", VERSION_RECT_SIZE)
        .attr("x", 0)
        .attr("y", 10)
        .attr("fill", d => stringToColor(d));

      versionGroup
        .append("text")
        .attr("class", "legendText")
        .attr("x", LEGEND_WIDTH)
        .attr("y", 20)
        .text(d => d);

      versionNodes.exit().remove();
    }
    ReactTooltip.rebuild();

    let doResize;
    window.onresize = () => {
      clearTimeout(doResize);
      doResize = setTimeout(() => this.drawViz(this.props, this.state), 500);
    };
  }

  componentDidMount() {
    this.listenForUpdates(this.props);
  }

  componentDidUpdate() {
    this.drawViz(this.props, this.state);
  }

  componentWillReceiveProps(nextProps: Props) {
    this.listenForUpdates(nextProps);
  }

  componentWillUnmount() {
    const { eventSource } = this.state;
    eventSource && eventSource.close();
  }

  updateData(json) {
    this.setState({
      data: json
    });
  }

  timeShift(nextBack: "next" | "back") {
    const { drillDown, start, end } = this.props;
    const hours = Math.max(
      6,
      Math.floor(
        moment.duration(moment.utc(end).diff(moment.utc(start))).asHours() / 4
      )
    );
    drillDown(
      moment.utc(start).add((nextBack == "next" ? 1 : -1) * hours, "hours"),
      moment.utc(end).add((nextBack == "next" ? 1 : -1) * hours, "hours")
    );
  }

  zoomOut() {
    const { drillDown, start, end } = this.props;
    const hours = Math.max(
      6,
      Math.floor(
        moment.duration(moment.utc(end).diff(moment.utc(start))).asHours() / 2
      )
    );
    drillDown(
      moment.utc(start).add(-hours, "hours"),
      moment.utc(end).add(hours, "hours")
    );
  }

  zoomIn() {
    const { drillDown, start, end } = this.props;
    const hours = Math.max(
      6,
      Math.floor(
        moment.duration(moment.utc(end).diff(moment.utc(start))).asHours() / 4
      )
    );
    if (hours > 6)
      drillDown(
        moment.utc(start).add(hours, "hours"),
        moment.utc(end).add(-hours, "hours")
      );
  }

  render() {
    let { classes, start, end, selectedJobs } = this.props;
    let { data } = this.state;
    return (
      <div className={classes.container}>
        <h1 className={classes.title}>
          <Link href="/timeseries/calendar">Calendar</Link>{" "}
          <ChevronIcon className={classes.chevron} />
          <span className={classes.range}>
            {formatDate(start)} <ToIcon className={classes.to} />{" "}
            {formatDate(end)} UTC
          </span>
        </h1>
        <div>
          <div className={classes.showVersion}>
            <label className={classes.showVersionLabel}>Show versions for each execution</label>
            <input
              name="versionCheckBox"
              type="checkbox"
              checked={this.state.showVersion}
              onChange={() => this.toggleShowVersion()}
            />
          </div>
          <div className={classes.timeControl}>
            <ArrowPrevious
              onClick={() => this.timeShift("back")}
              className="button"
            />
            <ZoomIn
              className="button"
              style={{ fontSize: "1.5em" }}
              onClick={() => this.zoomIn()}
            />
            <ZoomOut
              className="button"
              style={{ fontSize: "1.5em" }}
              onClick={() => this.zoomOut()}
            />
            <ArrowNext
              onClick={() => this.timeShift("next")}
              className="button"
            />
          </div>
        </div>
        {data && data.summary.length ? (
          <div className={classes.graph} ref={r => (this.vizContainer = r)}>
            <div className={classes.summarySvg}>
              <svg ref={r => (this.summarySvgContainer = r)}>
                <g id="axisContainer" />
                <g id="summary" />
              </svg>
            </div>
            <div className={classes.detailsSvg}>
              <svg ref={r => (this.detailsSvgContainer = r)} />
              <div className={classes.legendContainer}>
                <svg ref={r => (this.legendContainer = r)} />
              </div>
            </div>
            <ReactTooltip
              className={classes.tooltip}
              effect="float"
              html={true}
            />
          </div>
        ) : data ? (
          <div className={classes.noData}>
            <div>
              Nothing to be done for this period
              {selectedJobs.length ? " (some may have been filtered)" : ""}
            </div>
          </div>
        ) : (
          <Spinner />
        )}
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
  title: {
    fontSize: "1.2em",
    margin: "0 0 16px 0",
    color: "#607e96",
    fontWeight: "normal"
  },
  timeControl: {
    fontSize: "0.8em",
    float: "right",
    textAlign: "right",
    height: "30px",
    width: "160px",
    color: "#607e96",
    "& .button": {
      cursor: "pointer",
      margin: "0 0.5em"
    }
  },
  showVersion: {
    display: "flex",
    alignItems: "center",
    fontSize: "0.8em",
    float: "left",
    height: "30px",
    color: "#607e96"
  },
  showVersionLabel: {
    paddingRight: "0.5em"
  },
  chevron: {
    color: "#92a2af",
    transform: "scale(1.2) translateX(-2px)",
    padding: "0 .25em"
  },
  to: {
    color: "#92a2af",
    transform: "scale(1) translateY(-1px)"
  },
  range: {
    fontSize: ".8em",
    transform: "translateY(1px)"
  },
  graph: {
    background: "#fff",
    padding: "2em 0",
    borderRadius: "2px",
    boxShadow: "0px 1px 2px #BECBD6",
    flex: "1",
    display: "flex",
    flexDirection: "column"
  },
  axis: {
    "& path.domain, & line": {
      stroke: "#92a2af"
    },
    "& text": {
      fill: "#607e96",
      fontSize: "1.25em"
    }
  },
  noData: {
    flex: "1",
    textAlign: "center",
    fontSize: "0.9em",
    color: "#8089a2",
    paddingBottom: "15%",
    display: "flex",
    "& > div": {
      alignSelf: "center",
      flex: 1
    }
  },
  period: {
    cursor: "pointer",
    transition: ".1s",
    "&:hover": {}
  },
  aggregatedPeriod: {
    extend: "period",
    cursor: "default"
  },
  jobName: {
    fontSize: "0.75em",
    fill: "#607e96"
  },
  summarySvg: {},
  detailsSvg: {
    borderTop: "1px dashed #ecf1f5",
    paddingTop: "30px",
    flex: "1",
    overflowX: "hidden"
  },
  legendContainer: {
    float: "right"
  },
  tooltip: {
    backgroundColor: "#2b3346 !important",
    border: "4px solid #171c2b",
    color: "#ffffff",
    padding: "1em !important",
    boxShadow: "0px 0px 10px rgba(255, 255, 255, 0.5)",
    "&.place-top::after": {
      borderTopColor: "#171c2b !important",
      bottom: "-10px !important",
      transform: "translateX(-4px)"
    },
    "&.place-left::after": {
      borderLeftColor: "#171c2b !important",
      right: "-10px !important"
    }
  }
};

let f = "YYYY-MM-DDTHH";
const mapStateToProps = ({ app: { selectedJobs, workflow } }) => ({
  selectedJobs,
  workflow
});
const mapDispatchToProps = dispatch => ({
  drillDown(start, end) {
    dispatch(
      navigate(`/timeseries/calendar/${start.format(f)}Z_${end.format(f)}Z`)
    );
  },
  showExecutions(job, start, end) {
    dispatch(
      navigate(
        `/timeseries/executions/${job}/${start.format(f)}Z_${end.format(f)}Z`
      )
    );
  }
});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(CalendarFocus)
);
