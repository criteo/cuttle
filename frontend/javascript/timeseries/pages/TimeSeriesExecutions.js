// @flow

import React from "react";
import ReactDOM from "react-dom";
import { connect } from "react-redux";
import classNames from "classnames";
import injectSheet from "react-jss";
import OpenIcon from "react-icons/lib/md/zoom-in";
import moment from "moment";
import _ from "lodash";

import Window from "../../common/components/Window";
import Table from "../../common/components/Table";
import FancyTable from "../../common/components/FancyTable";
import Spinner from "../../common/components/Spinner";
import Link from "../../common/components/Link";
import Clock from "../../common/components/Clock";
import Status from "../../common/components/Status";
import { Badge } from "../../common/components/Badge";
import { listenEvents } from "../../common/Utils";
import type { ExecutionLog, Job } from "../datamodel";

type Props = {
  classes: any,
  envCritical: boolean,
  job: Job,
  start: string,
  end: string
};

type State = {
  query: ?string,
  data: ?{
    jobExecutions: Array<ExecutionLog>,
    parentExecutions: Array<ExecutionLog>
  },
  eventSource: ?any,
  sort: {
    column: string,
    order: "asc" | "desc"
  },
  blockedBySelectedStatus?: string
};

let formatDate = (date: string) =>
  moment(date)
    .utc()
    .format("YYYY-MM-DD HH:mm");

class TimeSeriesExecutions extends React.Component<Props, State> {
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
    let newQuery = `/api/timeseries/executions?job=${job.id}&start=${moment(
      start
    ).toISOString()}&end=${moment(end).toISOString()}`;
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
      sort: {
        column,
        order:
          this.state.sort.column == column && this.state.sort.order == "asc"
            ? "desc"
            : "asc"
      }
    });
  }

  render() {
    let { classes, job, start, end, envCritical } = this.props;
    let { data, sort } = this.state;

    const sortedJobExecutions = jobExecutions => {
      let result = _.sortBy(jobExecutions, e => {
        switch (sort.column) {
          case "backfill":
            return e.context.backfill;
          default:
            return e.endTime;
        }
      });
      if (sort.order == "desc") {
        result = _.reverse(result);
      }
      return result;
    };

    const blockedByRow = (executions: Array<ExecutionLog>) => {
      const groupedParentExecutions = (executions: Array<ExecutionLog>) =>
        _(executions)
          .countBy(e => e.status)
          .entries()
          .sortBy(([status, _]) => status)
          .map(([status, count]) => (
            <a
              className={classes.openIcon}
              onClick={() => {
                this.setState({
                  blockedBySelectedStatus:
                    this.state.blockedBySelectedStatus == status
                      ? undefined
                      : status
                });
              }}
              style={{ cursor: "pointer" }}
            >
              <Status
                className={classes.executionBadge}
                key={status}
                status={status}
                labelFormatter={s => `${count} - ${s}`}
              />
            </a>
          ))
          .value();
      const blockedByExpandedRow = (
        executions: Array<ExecutionLog>,
        status: string
      ) =>
        _(executions)
          .filter(e => e.status === status)
          .sortBy("id")
          .sortBy("job")
          .map(e => {
            const href =
              e.status === "todo"
                ? `/timeseries/executions/${e.job}/${e.context.start}_${
                    e.context.end
                  }`
                : `/executions/${e.id}`;

            return (
              <Link href={href}>
                <Status
                  className={classes.executionBadge}
                  status={e.status}
                  labelFormatter={_ => e.job}
                />
              </Link>
            );
          })
          .value();

      if (executions.length) {
        return [
          <dt key="incomplete_parents">Blocked by:</dt>,
          <dd key="incomplete_parents_">
            {groupedParentExecutions(executions)}
            {this.state.blockedBySelectedStatus ? (
              <div>
                {blockedByExpandedRow(
                  executions,
                  this.state.blockedBySelectedStatus
                )}
              </div>
            ) : null}
          </dd>
        ];
      } else return null;
    };

    return (
      <Window title="Executions for the period">
        {data ? (
          [
            <FancyTable key="properties">
              <dt key="job">Job:</dt>
              <dd key="job_">
                <Link href={`/workflow/${job.id}`}>{job.name}</Link>
              </dd>
              <dt key="start">Period start:</dt>
              <dd key="start_">{formatDate(start)} UTC</dd>
              <dt key="end">Period end:</dt>
              <dd key="end_">{formatDate(end)} UTC</dd>
              <dt key="backfilled">Backfilled:</dt>
              <dd key="backfilled_">
                {data.jobExecutions.filter(e => e.context.backfill).length
                  ? "Yes"
                  : "No"}
              </dd>
              {blockedByRow(data.parentExecutions)}
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
                data={sortedJobExecutions(data.jobExecutions)}
                render={(
                  column,
                  { id, startTime, endTime, status, context }
                ) => {
                  switch (column) {
                    case "backfill":
                      return context.backfill ? (
                        <Link
                          href={`/timeseries/backfills/${context.backfill.id}`}
                        >
                          <Badge label="BACKFILL" kind="alt" width={75} />
                        </Link>
                      ) : (
                        <span className={classes.missing}>no</span>
                      );
                    case "startTime":
                      return startTime ? (
                        <Clock
                          className={classes.time}
                          time={startTime || ""}
                        />
                      ) : (
                        <span className={classes.missing}>not yet</span>
                      );
                    case "endTime":
                      return endTime ? (
                        <Clock className={classes.time} time={endTime || ""} />
                      ) : (
                        <span className={classes.missing}>not yet</span>
                      );
                    case "status":
                      return id ? (
                        <Link
                          className={classes.openIcon}
                          href={`/executions/${id}`}
                        >
                          <Status status={status} />
                        </Link>
                      ) : (
                        <Status status={status} />
                      );
                    case "detail":
                      return id ? (
                        <Link
                          className={classes.openIcon}
                          href={`/executions/${id}`}
                        >
                          <OpenIcon />
                        </Link>
                      ) : (
                        <OpenIcon
                          className={classNames([
                            classes.openIcon,
                            classes.disabled
                          ])}
                        />
                      );
                  }
                }}
              />
            </div>
          ]
        ) : (
          <Spinner />
        )}
      </Window>
    );
  }
}

const styles = {
  logs: {
    flex: "1",
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
  },
  executionBadge: {
    marginRight: "5px"
  }
};

const mapStateToProps = ({ app: { project } }) => ({
  envCritical: project.env.critical
});
const mapDispatchToProps = () => ({});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(injectSheet(styles)(TimeSeriesExecutions));
