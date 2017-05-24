// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
import Measure from "react-measure";
import _ from "lodash";
import moment from "moment";

import ReactPaginate from "react-paginate";
import PrevIcon from "react-icons/lib/md/navigate-before";
import NextIcon from "react-icons/lib/md/navigate-next";
import BreakIcon from "react-icons/lib/md/keyboard-control";
import AscIcon from "react-icons/lib/md/keyboard-arrow-down";
import DescIcon from "react-icons/lib/md/keyboard-arrow-up";
import OpenIcon from "react-icons/lib/md/launch";
import CalendarIcon from "react-icons/lib/md/date-range";

import Spinner from "../components/Spinner";
import Clock from "../components/Clock";
import { listenEvents } from "../../Utils";
import type { Paginated, ExecutionLog, Workflow } from "../../datamodel";
import { Badge } from "../components/Badge";

type Props = {
  classes: any,
  className: string,
  workflow: Workflow,
  request: (
    page: number,
    rowsPerPage: number,
    sort: { column: string, order: "asc" | "desc" }
  ) => string,
  columns: Array<| "job"
    | "context"
    | "startTime"
    | "endTime"
    | "status"
    | "detail">,
  label: string,
  defaultSort: {
    column: string,
    order: "asc" | "desc"
  }
};

type State = {
  data: ?Array<ExecutionLog>,
  page: number,
  total: number,
  sort: {
    column: string,
    order: "asc" | "desc"
  },
  rowsPerPage: number,
  query: ?string,
  eventSource: any
};

const ROW_HEIGHT = 43;

class ExecutionLogs extends React.Component {
  props: Props;
  state: State;

  constructor(props: Props) {
    super(props);
    this.state = {
      data: null,
      page: 0,
      total: -1,
      sort: props.defaultSort,
      rowsPerPage: 25,
      query: null,
      eventSource: null
    };
    (this: any).adaptTableHeight = _.throttle(
      this.adaptTableHeight.bind(this),
      1000
    );
  }

  componentDidUpdate() {
    let { query, page, rowsPerPage, eventSource, sort } = this.state;
    let newQuery = this.props.request(page, rowsPerPage, sort);
    if (newQuery != query) {
      eventSource && eventSource.close();
      eventSource = listenEvents(newQuery, this.updateData.bind(this));
      this.setState({
        ...this.state,
        query: newQuery,
        data: null,
        eventSource
      });
    }
  }

  componentWillUnmount() {
    let { eventSource } = this.state;
    eventSource && eventSource.close();
  }

  adaptTableHeight({ height }) {
    this.setState({
      ...this.state,
      rowsPerPage: Math.max(1, Math.floor(height / ROW_HEIGHT) - 1)
    });
  }

  updateData(json: Paginated<ExecutionLog>) {
    this.setState({
      ...this.state,
      total: json.total,
      page: Math.min(
        this.state.page,
        Math.ceil(json.total / this.state.rowsPerPage) - 1
      ),
      data: json.data
    });
  }

  changePage({ selected }) {
    this.setState({
      ...this.state,
      page: selected
    });
  }

  sortBy(column: string) {
    if (column == this.state.sort.column) {
      this.setState({
        ...this.state,
        sort: {
          column,
          order: this.state.sort.order == "asc" ? "desc" : "asc"
        },
        page: 0
      });
    } else {
      this.setState({
        ...this.state,
        sort: {
          column,
          order: "asc"
        },
        page: 0
      });
    }
  }

  render() {
    let { data, page, rowsPerPage, total, sort } = this.state;
    let { classes, workflow, label } = this.props;

    let jobName = (id: string) => {
      let job = workflow.getJob(id);
      if (job) {
        return job.name;
      } else {
        return id;
      }
    };

    let ColumnHeader = (
      {
        label,
        width,
        sortBy
      }: { label?: string, width?: number, sortBy?: string }
    ) => {
      if (sortBy) {
        let isSorted = sort.column == sortBy ? sort.order : null;
        return (
          <th
            width={width || "auto"}
            onClick={this.sortBy.bind(this, sortBy)}
            className={classes.sortable}
          >
            {label}
            {isSorted == "asc"
              ? <AscIcon className={classes.sortIcon} />
              : null}
            {isSorted == "desc"
              ? <DescIcon className={classes.sortIcon} />
              : null}
            {!isSorted
              ? <AscIcon
                  className={classes.sortIcon}
                  style={{ color: "transparent" }}
                />
              : null}
          </th>
        );
      } else {
        return <th width={width || "auto"}>{label}</th>;
      }
    };

    let Context = ({ ctx }) => {
      // Need to be dynamically linked with the scehduler but for now let's
      // assume that it is a TimeseriesContext
      let format = date => moment(date).utc().format("MMM-DD HH:mm") + " UTC";
      return (
        <a href={`/timeries/calendar/${ctx.start}-${ctx.end}`}>
          <CalendarIcon
            style={{
              fontSize: "1.2em",
              verticalAlign: "top",
              transform: "translateY(-1px)"
            }}
          />
          {" "}
          {format(ctx.start)}
          {" "}
          <BreakIcon />
          {" "}
          {format(ctx.end)}
        </a>
      );
    };

    let Table = () => {
      if (data) {
        return (
          <table className={classes.table}>
            <thead>
              <tr>
                {this.props.columns.map(column => {
                  switch (column) {
                    case "job":
                      return (
                        <ColumnHeader key="job" label="Job" sortBy="job" />
                      );
                    case "context":
                      return (
                        <ColumnHeader
                          key="context"
                          label="Context"
                          sortBy="context"
                        />
                      );
                    case "failed":
                      return (
                        <ColumnHeader
                          key="failed"
                          label="Failed"
                          sortBy="failed"
                        />
                      );
                    case "retry":
                      return (
                        <ColumnHeader
                          key="retry"
                          label="Next retry"
                          sortBy="retry"
                        />
                      );
                    case "startTime":
                      return (
                        <ColumnHeader
                          key="startTime"
                          label="Started"
                          sortBy="startTime"
                        />
                      );
                    case "endTime":
                      return (
                        <ColumnHeader
                          key="endTime"
                          label="Finished"
                          sortBy="endTime"
                        />
                      );
                    case "status":
                      return (
                        <ColumnHeader
                          key="status"
                          label="Status"
                          width={120}
                          sortBy="status"
                        />
                      );
                    case "detail":
                      return <ColumnHeader key="detail" width={40} />;
                  }
                })}
              </tr>
            </thead>
            <tbody>
              {data.map(({
                id,
                job,
                startTime,
                endTime,
                status,
                context,
                failing
              }) => (
                <tr key={id}>
                  {this.props.columns.map(column => {
                    switch (column) {
                      case "job":
                        return (
                          <td key="job">
                            <a href={`/workflow/${job}`}>{jobName(job)}</a>
                          </td>
                        );
                      case "context":
                        return <td key="context"><Context ctx={context} /></td>;
                      case "failed":
                        let times = (failing &&
                          failing.failedExecutions.length) ||
                          0;
                        if (times == 1) {
                          return <td key="failed">Once</td>;
                        } else if (times > 1) {
                          return <td key="failed">{times} times</td>;
                        }
                      case "startTime":
                        return (
                          <td key="startTime">
                            <Clock className={classes.time} time={startTime} />
                          </td>
                        );
                      case "endTime":
                        return (
                          <td key="endTime">
                            <Clock
                              className={classes.time}
                              time={endTime || ""}
                            />
                          </td>
                        );
                      case "retry":
                        return (
                          <td key="retry">
                            <Clock
                              className={classes.time}
                              time={(failing && failing.nextRetry) || ""}
                            />
                          </td>
                        );
                      case "status":
                        if (status == "running") {
                          return (
                            <td key="status">
                              <Badge label="STARTED" kind="info" width={75} />
                            </td>
                          );
                        } else if (
                          status == "throttled" || status == "failed"
                        ) {
                          return (
                            <td key="status">
                              <Badge label="FAILED" kind="error" width={75} />
                            </td>
                          );
                        } else if (status == "successful") {
                          return (
                            <td key="status">
                              <Badge
                                label="SUCCESS"
                                kind="success"
                                width={75}
                              />
                            </td>
                          );
                        }
                      case "detail":
                        return (
                          <td key="detail">
                            <a
                              className={classes.openIcon}
                              href={`/executions/${id}`}
                            >
                              <OpenIcon />
                            </a>
                          </td>
                        );
                    }
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        );
      } else {
        return <Spinner />;
      }
    };

    let Pagination = () => {
      if (total >= 2 && total <= rowsPerPage) {
        return (
          <div className={classes.footer}>{`${total} ${label} executions`}</div>
        );
      } else if (total > rowsPerPage) {
        let pageCount = Math.ceil(total / rowsPerPage);
        return (
          <div className={classes.footer}>
            {`${page * rowsPerPage + 1} to ${Math.min(total, page * rowsPerPage + rowsPerPage)} of ${total} ${label} executions`}
            <ReactPaginate
              pageCount={pageCount}
              pageRangeDisplayed={3}
              marginPagesDisplayed={2}
              forcePage={page}
              previousLabel={<PrevIcon className={classes.paginationIcon} />}
              nextLabel={<NextIcon className={classes.paginationIcon} />}
              breakLabel={<BreakIcon className={classes.paginationIcon} />}
              containerClassName={classes.pagination}
              activeClassName={classes.paginationActive}
              onPageChange={this.changePage.bind(this)}
            />
          </div>
        );
      } else {
        return <div className={classes.footer} />;
      }
    };

    return (
      <div className={classes.container}>
        <Measure onMeasure={this.adaptTableHeight}>
          <div className={classes.data}><Table /></div>
        </Measure>
        <Pagination />
      </div>
    );
  }
}

const styles = {
  container: {
    flex: "1",
    display: "flex",
    flexDirection: "column"
  },
  data: {
    display: "flex",
    flex: "1"
  },
  table: {
    borderSpacing: "0",
    fontSize: ".9em",
    width: "100%",
    background: "#ffffff",
    "& thead": {
      color: "#303a41",
      boxShadow: "0px 1px 2px #BECBD6"
    },
    "& tr": {
      height: ROW_HEIGHT,
      padding: "0",
      textAlign: "left",
      boxSizing: "border-box",
      "&:hover": {
        background: "rgba(255, 215, 0, 0.1)"
      }
    },
    "& th": {
      padding: "0 15px",
      background: "#f5f8fa",
      height: "46px",
      boxSizing: "border-box",
      cursor: "default"
    },
    "& td": {
      padding: "0 15px",
      borderBottom: "1px solid #ecf1f5",
      "& a": {
        color: "inherit",
        textDecoration: "none"
      }
    }
  },
  sortable: {
    cursor: "pointer !important",
    userSelect: "none"
  },
  sortIcon: {
    color: "#6f98b1"
  },
  time: {
    color: "#85929c"
  },
  openIcon: {
    fontSize: "16px",
    color: "#607e96",
    padding: "15px",
    margin: "-15px"
  },
  footer: {
    display: "flex",
    height: "2em",
    margin: ".8em 0 0 0",
    lineHeight: "2em",
    fontSize: ".9em",
    color: "#8089a2",
    background: "#ecf1f5"
  },
  pagination: {
    margin: "0",
    flex: "1",
    textAlign: "right",
    transform: "translateX(1em)",
    "& li": {
      display: "inline-block"
    },
    "& li a": {
      padding: "10px",
      cursor: "pointer",
      userSelect: "none",
      outline: "none"
    }
  },
  paginationActive: {
    background: "#d6dfe6",
    color: "#4a6880",
    borderRadius: "2px"
  },
  paginationIcon: {
    fontSize: "1.5em",
    transform: "translateY(-1px)"
  }
};

export default injectSheet(styles)(ExecutionLogs);
