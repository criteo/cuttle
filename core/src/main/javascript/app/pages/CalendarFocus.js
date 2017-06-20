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

import Link from "../components/Link";
import Spinner from "../components/Spinner";

import { listenEvents } from "../../Utils";

type Props = {
  classes: any,
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
  summary: Array<[Period, [number, boolean]]>,
  jobs: { [job: string]: Array<[Period, "done" | "running" | "todo"]> }
};

type State = {
  data: ?Stats,
  query: ?string,
  eventSource: ?any
};

let formatDate = (date: string) =>
  moment(date).utc().format("YYYY-MM-DD HH:mm");

class CalendarFocus extends React.Component {
  props: Props;
  state: State;
  vizContainer: any;

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
    let jobsFilter = props.selectedJobs.length
      ? `&jobs=${props.selectedJobs.join(",")}`
      : "";
    let query = `/api/timeseries/calendar/focus?start=${moment(start).toISOString()}&end=${moment(end).toISOString()}${jobsFilter}`;
    if (this.state.query != query) {
      this.state.eventSource && this.state.eventSource.close();
      let eventSource = listenEvents(query, this.updateData.bind(this));
      this.setState({
        ...this.state,
        data: null,
        eventSource
      });
    }
  }

  drawViz() {
    let { data } = this.state;
    let { classes, start, end } = this.props;
    if (data && data.summary.length) {
      let { summary, jobs } = data;

      let width = this.vizContainer.clientWidth;
      let summarySvg = d3
        .select(this.vizContainer)
        .select(`div.${classes.summarySvg}`)
        .html(null)
        .append("svg")
        .attr("width", width)
        .attr("height", 80);

      let LEFT_MARGIN = 30;
      let RIGHT_MARGIN = 50;
      let ROW_HEIGHT = 20;
      let PADDING = 5;
      let MARGIN = 1;

      // compute label sizes
      let globalLabel = "Workflow";
      let g = summarySvg.append("g");
      _.entries(jobs).concat([[globalLabel, null]]).forEach(([job]) => {
        g.append("text").attr("class", classes.jobName).text(job);
      });
      let labelWidth = g.node().getBBox().width + LEFT_MARGIN;
      summarySvg.html(null);

      // Time axis
      let axisWidth = width - labelWidth - RIGHT_MARGIN;
      let timeScale = d3
        .scaleUtc()
        .domain([moment(start).toDate(), moment(end).toDate()])
        .range([0, axisWidth]);
      let colorScale = d3.interpolateRgb("#ecf1f5", "#62cc64");
      let tickFormat = date => {
        return (d3.utcHour(date) < date
          ? d3.utcFormat("")
          : d3.utcDay(date) < date
              ? d3.utcFormat("%H:00")
              : d3.utcFormat("%Y-%m-%d"))(date);
      };
      let timeAxis = d3.axisTop(timeScale).tickFormat(tickFormat).ticks(5);
      summarySvg
        .append("g")
        .attr("transform", `translate(${labelWidth}, ${ROW_HEIGHT + PADDING})`)
        .attr("class", classes.axis)
        .call(timeAxis);

      // Summary
      let drawSummary = root => {
        summary.forEach(([period, [completion, error]]) => {
          let x1 = timeScale(moment(period.start)),
            x2 = timeScale(moment(period.end));
          let g = root
            .append("g")
            .attr(
              "data-tip",
              `
              <div>
                ${Math.ceil(completion * 100)}% complete –
                ${formatDate(period.start)} to ${formatDate(period.end)} UTC
              </div>
            `
            )
            .attr("class", classes.aggregatedPeriod)
            .attr("transform", `translate(${x1 + MARGIN + 2}, 0)`);
          g
            .append("rect")
            .attr("y", -(ROW_HEIGHT / 2))
            .attr("x", -4)
            .attr("width", x2 - x1 - MARGIN * 2 + 4)
            .attr("height", 1.5 * ROW_HEIGHT)
            .attr("fill", "transparent");
          g
            .append("rect")
            .attr("width", x2 - x1 - MARGIN * 2 - 4)
            .attr("height", ROW_HEIGHT - 4)
            .attr(
              "fill",
              error ? "#e91e63" : completion == 1 ? "#62cc64" : "#ecf1f5"
            )
            .attr("stroke-width", "4")
            .attr("stroke", error ? "#e91e63" : colorScale(completion));
        });
      };
      let summaryGroup = summarySvg
        .append("g")
        .attr("id", "summary")
        .attr("transform", `translate(${labelWidth}, 35)`);
      summaryGroup
        .append("text")
        .attr("class", classes.jobName)
        .attr("text-anchor", "end")
        .attr("x", -(PADDING * 2))
        .attr("y", 14)
        .attr("height", 0)
        .text(globalLabel);
      summaryGroup.call(drawSummary);

      // Breakdown
      let detailsSvg = d3
        .select(this.vizContainer)
        .select(`.${classes.detailsSvg}`)
        .html(null)
        .append("svg")
        .attr("width", width)
        .attr("height", _.entries(jobs).length * (ROW_HEIGHT + 2 * MARGIN));
      let drawJob = (root, job, jobStats) => {
        jobStats.forEach(([period, status]) => {
          let x1 = timeScale(moment(period.start)),
            x2 = timeScale(moment(period.end));
          let g = root
            .append("g")
            .attr(
              "data-tip",
              `
              <div>
                ${job} is ${status == "failed" ? "stuck" : status == "successful" ? "done" : status == "running" ? "started" : "todo"}
                – ${formatDate(period.start)} to ${formatDate(period.end)} UTC
              </div>
            `
            )
            .attr("class", classes.period)
            .attr("transform", `translate(${x1 + MARGIN}, 0)`)
            .on("click", () =>
              this.props.showExecutions(
                job,
                moment.utc(period.start),
                moment.utc(period.end)
              )
            );
          g
            .append("rect")
            .attr("width", x2 - x1)
            .attr("height", ROW_HEIGHT + 2 * MARGIN)
            .attr("fill", "transparent");
          g
            .append("rect")
            .attr("width", x2 - x1 - 2 * MARGIN)
            .attr("height", ROW_HEIGHT)
            .attr(
              "fill",
              status == "failed"
                ? "#e91e63"
                : status == "successful"
                    ? "#62cc64"
                    : status == "waiting"
                        ? "#ffbc5a"
                        : status == "running" ? "#49d3e4" : "#ecf1f5"
            );
        });
      };
      let drawJobs = root => {
        _.sortBy(_.entries(jobs), e => e[0]).forEach(([job, data], i) => {
          let g = root
            .append("g")
            .attr(
              "transform",
              `translate(${labelWidth}, ${i * (ROW_HEIGHT + 2 * MARGIN)})`
            );
          g
            .append("text")
            .attr("class", classes.jobName)
            .attr("text-anchor", "end")
            .attr("x", -(2 * PADDING))
            .attr("y", 14)
            .text(job);
          g.call(drawJob, job, data);
        });
      };
      detailsSvg.append("g").attr("id", "detail").call(drawJobs);
    }
    ReactTooltip.rebuild();
  }

  componentDidMount() {
    this.listenForUpdates(this.props);
  }

  componentDidUpdate() {
    this.drawViz();
  }

  componentWillReceiveProps(nextProps: Props) {
    this.listenForUpdates(nextProps);
  }

  componentWillUnmount() {
    let { eventSource } = this.state;
    eventSource && eventSource.close();
  }

  updateData(json: Stats) {
    this.setState({
      ...this.state,
      data: json
    });
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
        </h1>
        {data && data.summary.length
          ? <div className={classes.graph} ref={r => (this.vizContainer = r)}>
              <div className={classes.summarySvg} />
              <div className={classes.detailsSvg} />
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
const mapStateToProps = ({ app: { selectedJobs } }) => ({ selectedJobs });
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
