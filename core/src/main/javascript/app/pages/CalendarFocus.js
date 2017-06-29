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

type Stats = {
  summary: Array<{
    period: Period,
    completion: number,
    error: boolean,
    backfill: boolean
  }>,
  jobs: {
    [job: string]: {
      period: Period,
      status: "done" | "running" | "todo",
      backfill: boolean
    }
  }
};

type State = {
  data: ?Stats,
  query: ?string,
  eventSource: ?any
};

// Static/format helpers
let globalLabel = "Workflow";

let LEFT_MARGIN = 30;
let RIGHT_MARGIN = 50;
let ROW_HEIGHT = 20;
let PADDING = 5;
let MARGIN = 1;

let formatDate = (date: string) =>
  moment(date).utc().format("YYYY-MM-DD HH:mm");

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
    g.append("text").attr("class", jobNameClass).text(job);
  });
  let labelWidth = g.node().getBBox().width + LEFT_MARGIN;
  svg.select("g#widthHack").remove();
  return labelWidth;
};

// Drawing methods // Summary periods
const summaryPeriodHelper = (x1, x2) => ({
  tip: ({ period, completion }) =>
    `<div>${Math.ceil(completion * 100)}% complete – ${formatDate(period.start)} to ${formatDate(period.end)} UTC</div>`,
  translate: ({ period }) => `translate(${x1(period) + MARGIN + 2}, 0)`,
  width: ({ period }) => x2(period) - x1(period) - MARGIN * 2 - 4,
  fill: ({ error, completion }) =>
    error ? "#e91e63" : completion == 1 ? "#62cc64" : "#ecf1f5",
  stroke: ({ error, completion }) =>
    error ? "#e91e63" : colorScale(completion)
});

const summaryEnterBackfill = (enterBackfill, width) => {
  enterBackfill
    .append("rect")
    .attr("class", "backfill")
    .attr("width", width)
    .attr("height", 3)
    .attr("fill", "#bb65ca")
    .attr("x", -2)
    .attr("y", ROW_HEIGHT);
};

const drawSummary = (enterSelection, periodClass, x1, x2) => {
  const helper = summaryPeriodHelper(x1, x2);
  const newPeriodSlotNode = enterSelection
    .append("g")
    .attr("class", classNames("periodSlot", periodClass))
    .attr("data-tip", helper.tip)
    .attr("transform", helper.translate);
  newPeriodSlotNode
    .append("rect")
    .attr("class", "placeholder")
    .attr("y", -(ROW_HEIGHT / 2))
    .attr("x", -4)
    .attr("height", 1.5 * ROW_HEIGHT)
    .attr("fill", "transparent")
    .attr("width", helper.width);
  newPeriodSlotNode
    .append("rect")
    .attr("class", "border")
    .attr("height", ROW_HEIGHT - 4)
    .attr("width", helper.width)
    .attr("stroke-width", "4")
    .attr("stroke", helper.stroke)
    .attr("fill", helper.fill);
  newPeriodSlotNode.each(({ backfill, period }, i, nodes) => {
    const backfillSelection = d3
      .select(nodes[i])
      .selectAll("rect.backfill")
      .data(backfill ? [period] : [], k => k.start);
    backfillSelection
      .enter()
      .call(summaryEnterBackfill, helper.width({ period }) + 4);
  });

  return newPeriodSlotNode;
};

// drawing methods // job details
const jobPeriodsHelper = (x1, x2, showExecutions) => ({
  tip: ({ period, status, jobName }) =>
    `<div>${jobName} is ${status == "failed" ? "stuck" : status == "successful" ? "done" : status == "running" ? "started" : "todo"} – ${formatDate(period.start)} to ${formatDate(period.end)} UTC</div>`,
  translate: ({ period }) => `translate(${x1(period) + MARGIN}, 0)`,
  width: ({ period }) => x2(period) - x1(period),
  fill: ({ status }) =>
    status == "failed"
      ? "#e91e63"
      : status == "successful"
          ? "#62cc64"
          : status == "waiting"
              ? "#ffbc5a"
              : status == "running" ? "#49d3e4" : "#ecf1f5",
  click: ({ period, jobName }) =>
    showExecutions(jobName, moment.utc(period.start), moment.utc(period.end))
});

const enterBackfill = (jobPeriod, width) => {
  jobPeriod
    .append("rect")
    .attr("class", "backfill")
    .attr("width", width)
    .attr("height", 3)
    .attr("fill", "#bb65ca")
    .attr("x", MARGIN)
    .attr("y", ROW_HEIGHT - 3);
};

const drawJobPeriods = (
  enterPeriodNode,
  periodClass,
  x1,
  x2,
  showExecutions
) => {
  const helper = jobPeriodsHelper(x1, x2, showExecutions);
  let newPeriodSlot = enterPeriodNode
    .append("g")
    .attr("class", classNames("periodSlot", periodClass))
    .attr("data-tip", helper.tip)
    .attr("transform", helper.translate);
  newPeriodSlot.on("click", helper.click);
  newPeriodSlot
    .append("rect")
    .attr("class", "placeholder")
    .attr("height", ROW_HEIGHT + 2 * MARGIN)
    .attr("fill", "transparent")
    .attr("width", helper.width);
  newPeriodSlot.each(({ period, backfill }, i, nodes) => {
    const jobPeriod = d3.select(nodes[i]);
    const width = helper.width({ period }) - 2 * MARGIN;
    jobPeriod
      .append("rect")
      .attr("class", "colored")
      .attr("y", MARGIN)
      .attr("x", MARGIN)
      .attr("height", ROW_HEIGHT - (backfill ? 6 : 0))
      .attr("width", width)
      .attr("fill", helper.fill);
    jobPeriod
      .selectAll("rect.backfill")
      .data(backfill ? [period] : [], k => k.start)
      .enter()
      .call(enterBackfill, width);
  });
};

class CalendarFocus extends React.Component {
  props: Props;
  state: State;

  vizContainer: any;
  summarySvgContainer: any;
  detailsSvgContainer: any;

  constructor(props: Props) {
    super(props);
    this.state = {
      data: null,
      query: null,
      eventSource: null
    };
  }

  listenForUpdates(props: Props) {
    let { start, end } = props;
    let jobsFilter = props.selectedJobs.length > 0
      ? `&jobs=${props.selectedJobs.join(",")}`
      : "";
    let query = `/api/timeseries/calendar/focus?start=${moment(start).toISOString()}&end=${moment(end).toISOString()}${jobsFilter}`;
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
      let labelWidth = getMaxLabelWidth(
        [..._.keys(jobs), globalLabel],
        summarySvg,
        classes.jobName
      );

      // Time axis
      let axisWidth = width - labelWidth - RIGHT_MARGIN;
      let timeScale = d3
        .scaleUtc()
        .domain([moment(start).toDate(), moment(end).toDate()])
        .range([0, axisWidth]);

      let timeAxis = d3.axisTop(timeScale).tickFormat(tickFormat).ticks(5);
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
        .selectAll("g.periodSlot")
        .data(summary, k => k.period.start)
        .enter()
        .call(drawSummary, classes.aggregatedPeriod, x1, x2);

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
          .text(d => d);

        newJobTimeline
          .selectAll("g.periodSlot")
          .data(
            jobName => _.map(jobs[jobName], p => ({ ...p, jobName })),
            k => k.period.start
          )
          .enter()
          .call(
            drawJobPeriods,
            classes.period,
            x1,
            x2,
            this.props.showExecutions
          );
      };

      detailsSvg
        .selectAll("g.jobTimeline")
        .data(workflow.jobs.map(job => job.id))
        .enter()
        .call(drawJobs);
    }
    ReactTooltip.rebuild();

    let doResize;
    window.onresize = () => {
      clearTimeout(doResize);
      doResize = setTimeout(
        () => this.drawViz(this.props, this.state),
        500
      );
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
      // TODO: simplify this step by modifying the backend directly
      data: {
        summary: _.map(
          json.summary,
          ([period, [completion, error, backfill]]) => ({
            period,
            completion,
            error,
            backfill
          })
        ),
        jobs: _.mapValues(json.jobs, v =>
          _.map(v, ([period, status, backfill]) => ({
            period,
            status,
            backfill
          }))
        )
      }
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
          <Link href="/timeseries/calendar">Calendar</Link>
          {" "}
          <ChevronIcon className={classes.chevron} />
          <span className={classes.range}>
            {formatDate(start)}
            {" "}
            <ToIcon className={classes.to} />
            {" "}
            {formatDate(end)}
            {" "}
            UTC
          </span>
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
        </h1>
        {data && data.summary.length
          ? <div className={classes.graph} ref={r => (this.vizContainer = r)}>
            <div className={classes.summarySvg}>
              <svg ref={r => (this.summarySvgContainer = r)}>
                <g id="axisContainer" />
                <g id="summary">
                  <text
                    className={classes.jobName}
                    textAnchor="end"
                    x={-(PADDING * 2)}
                    y="14"
                  >
                    {globalLabel}
                  </text>
                </g>
              </svg>
            </div>
            <div className={classes.detailsSvg}>
              <svg ref={r => (this.detailsSvgContainer = r)} />
            </div>
            <ReactTooltip
              className={classes.tooltip}
              effect="float"
              html={true}
            />
          </div>
          : data
          ? <div className={classes.noData}>
            <div>
              Nothing to be done for this period
              {selectedJobs.length
                ? " (some may have been filtered)"
                : ""}
            </div>
          </div>
          : <Spinner />}
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
const mapStateToProps = ({ app: { selectedJobs, workflow } }) => ({ selectedJobs, workflow });
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
