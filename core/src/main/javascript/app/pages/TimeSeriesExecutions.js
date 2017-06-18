// @flow

import React from "react";
import ReactDOM from "react-dom";
import { connect } from "react-redux";
import classNames from "classnames";
import injectSheet from "react-jss";
import OpenIcon from "react-icons/lib/md/zoom-in";
import moment from "moment";
import _ from "lodash";

import Window from "../components/Window";
import Table from "../components/Table";
import FancyTable from "../components/FancyTable";
import Error from "../components/Error";
import Spinner from "../components/Spinner";
import Link from "../components/Link";
import Clock from "../components/Clock";
import JobStatus from "../components/JobStatus";
import { listenEvents, getBoundingClientRect } from "../../Utils";
import type { ExecutionLog } from "../../datamodel";

type Props = {
  classes: any,
  envCritical: boolean,
  job: string,
  start: string,
  end: string
};

type State = {
  query: ?string,
  data: ?Array<ExecutionLog>,
  eventSource: ?any,
  sort: {
    column: string,
    order: "asc" | "desc"
  }
};

let formatDate = (date: string) =>
  moment(date).utc().format("YYYY-MM-DD HH:mm");

class TimeSeriesExecutions extends React.Component {
  props: Props;
  state: State;
  tableRef: any;

  constructor(props: Props) {
    super(props);
    this.state = {
      query: null,
      data: null,
      eventSource: null,
      sort: {
        column: "endTime",
        order: "desc"
      }
    };
  }

  listen() {
    let { query, eventSource } = this.state;
    let { job, start, end } = this.props;
    let newQuery = `/api/timeseries/executions?job=${job}&start=${moment(start).toISOString()}&end=${moment(end).toISOString()}`;
    if (newQuery != query) {
      eventSource && eventSource.close();
      eventSource = listenEvents(newQuery, this.updateData.bind(this));
      this.setState({
        query: newQuery,
        data: null,
        eventSource
      });
    }
  }

  updateData(json: any) {
    this.setState({
      data: json
    });
  }

  componentDidUpdate() {
    this.listen();
    // Fix table header (sorry for that, no way to do it in CSS...)
    if (this.tableRef) {
      // We basically copy the style dynamically computed by the layout
      // engine to a static fixed positionning.
      let table: any = ReactDOM.findDOMNode(this.tableRef);
      let tablePosition = table.getBoundingClientRect();
      let thead = table.querySelector("thead");
      let widths = _.map(
        table.querySelectorAll("th"),
        th => th.getBoundingClientRect().width
      );
      thead.style.position = "fixed";
      thead.style.top = `${tablePosition.top}px`;
      thead.style.left = `${tablePosition.left}px`;
      thead.style.width = `${tablePosition.width}px`;
      thead.style.zIndex = `99999`;
      _.zip(table.querySelectorAll("th"), widths).forEach(([th, width]) => {
        th.width = width;
      });
      table.querySelectorAll("tbody tr").forEach(tr => {
        _.zip(tr.querySelectorAll("td"), widths).forEach(([td, width]) => {
          td.style.padding = "0";
          let div = td.querySelector("div");
          div.style.boxSizing = `border-box`;
          div.style.width = `${width}px`;
        });
      });
    }
  }

  componentWillMount() {
    this.listen();
  }

  componentWillUnmount() {
    let { eventSource } = this.state;
    eventSource && eventSource.close();
  }

  sortBy(column) {
    this.setState({
      ...this.state,
      sort: {
        column,
        order: this.state.sort.column == column &&
          this.state.sort.order == "asc"
          ? "desc"
          : "asc"
      }
    });
  }

  render() {
    let { classes, job, start, end, envCritical } = this.props;
    let { data, sort } = this.state;

    let sortedData = _.sortBy(data, e => {
      switch (sort.column) {
        case "backfill":
          return e.context.backfill;
        default:
          return e.endTime;
      }
    });
    if (sort.order == "desc") {
      sortedData = _.reverse(sortedData);
    }

    return (
      <Window title="Executions for the period">
        {data
          ? [
              <FancyTable key="properties">
                <dt key="job">Job:</dt>
                <dd key="job_"><Link href={`/workflow/${job}`}>{job}</Link></dd>
                <dt key="start">Period start:</dt>
                <dd key="start_">{formatDate(start)} UTC</dd>
                <dt key="end">Period end:</dt>
                <dd key="end_">{formatDate(end)} UTC</dd>
                <dt key="backfilled">Backfilled:</dt>
                <dd key="backfilled_">
                  {data.filter(e => e.context.backfill).length ? "Yes" : "No"}
                </dd>
              </FancyTable>,
              <div
                key="data"
                className={classes.logs}
                ref={r => (this.tableRef = r)}
              >
                <Table
                  envCritical={envCritical}
                  columns={[
                    { id: "backfill", label: "Backfill", sortable: true },
                    { id: "startTime", label: "Started", sortable: true },
                    { id: "endTime", label: "Finished", sortable: true },
                    {
                      id: "status",
                      label: "Status",
                      width: 120,
                      sortable: true
                    },
                    { id: "detail", width: 40 }
                  ]}
                  onSortBy={this.sortBy.bind(this)}
                  sort={sort}
                  data={sortedData}
                  render={(column, { id, startTime, endTime, status }) => {
                    switch (column) {
                      case "backfill":
                        return <span className={classes.missing}>no</span>;
                      case "startTime":
                        return startTime
                          ? <Clock
                              className={classes.time}
                              time={startTime || ""}
                            />
                          : <span className={classes.missing}>not yet</span>;
                      case "endTime":
                        return endTime
                          ? <Clock
                              className={classes.time}
                              time={endTime || ""}
                            />
                          : <span className={classes.missing}>not yet</span>;
                      case "status":
                        return id
                          ? <Link
                              className={classes.openIcon}
                              href={`/executions/${id}`}
                            >
                              <JobStatus status={status} />
                            </Link>
                          : <JobStatus status={status} />;
                      case "detail":
                        return id
                          ? <Link
                              className={classes.openIcon}
                              href={`/executions/${id}`}
                            >
                              <OpenIcon />
                            </Link>
                          : <OpenIcon
                              className={classNames([
                                classes.openIcon,
                                classes.disabled
                              ])}
                            />;
                    }
                  }}
                />
              </div>
            ]
          : <Spinner />}
      </Window>
    );
  }
}

const styles = {
  logs: {
    flex: "1",
    margin: "1em -1em -1em -1em",
    overflow: "scroll",
    borderTop: "46px solid #fff"
  },
  openIcon: {
    fontSize: "22px",
    color: "#607e96",
    padding: "15px",
    margin: "-15px"
  },
  missing: {
    color: "#b2c0cc"
  },
  disabled: {
    opacity: ".25"
  }
};

const mapStateToProps = ({ app: { project } }) => ({
  envCritical: project.env.critical
});
const mapDispatchToProps = () => ({});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(TimeSeriesExecutions)
);
