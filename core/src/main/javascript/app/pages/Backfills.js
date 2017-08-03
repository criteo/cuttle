// @flow

import React from "react";
import injectSheet from "react-jss";
import { connect } from "react-redux";
import { compose } from "redux";
import { navigate } from "redux-url";

import OpenIcon from "react-icons/lib/md/zoom-in";

import BackfillStatus from "../components/BackfillStatus";
import Clock from "../components/Clock";
import Link from "../components/Link";
import TimeRangeLink from "../components/TimeRangeLink";
import PopoverMenu from "../components/PopoverMenu";
import Spinner from "../components/Spinner";
import Table from "../components/Table";
import { urlFormat } from "../utils/Date";

import type { Backfill, Statistics } from "../../datamodel";
import { backfillFromJSON } from "../../datamodel";

type Sort = {
  column: string,
  order: "asc" | "desc"
};

type Props = {
  classes: any,
  envCritical: boolean,
  sort: Sort,
  open: (link: string, replace: boolean) => void,
  statistics: Statistics,
  selectedJobs: Array<string> //TODO jobs filtering
};

type State = {
  data: ?Array<Backfill>,
  backfills: number
};

type BackfillSortFunction = (Backfill, Backfill) => number;

class Backfills extends React.Component {
  props: Props;
  state: State;

  constructor(props: Props) {
    super(props);
    this.state = {
      data: null,
      backfills: 0
    };
  }

  loadBackfills() {
    return fetch(`/api/timeseries/backfills`)
      .then((response: Response) => {
        if (!response.ok) throw new Error({ _error: response.statusText });
        return response.json();
      })
      .then(json => {
        let data: Array<Backfill> = json.map(backfillFromJSON);
        return this.setState({ ...this.state, data });
      });
    //TODO .catch & display error
  }

  componentWillMount() {
    this.loadBackfills();
  }

  componentWillReceiveProps(nextProps: Props) {
    let nextScheduler = nextProps.statistics.scheduler;
    let scheduler = this.props.statistics.scheduler;
    if (
      nextScheduler &&
      scheduler &&
      nextScheduler.backfills !== scheduler.backfills
    )
      this.loadBackfills();
  }

  qs = (sort: string, order: "asc" | "desc") => `?sort=${sort}&order=${order}`;

  sortBy(column: string) {
    let { sort } = this.props;
    let order = "asc";
    if (column == sort.column) {
      order = sort.order == "asc" ? "desc" : "asc";
    }
    this.props.open(this.qs(column, order), false);
  }

  static sortFunction(sort: Sort): BackfillSortFunction {
    const sortFn = (sort: Sort): BackfillSortFunction => {
      switch (sort.column) {
        case "name":
          return (a, b) => a.name.localeCompare(b.name);
        case "jobs":
          return (a, b) => b.jobs.length - a.jobs.length;
        case "period":
          return (a, b) => b.start.unix() - a.start.unix();
        case "status":
          return (a, b) => b.status.localeCompare(a.status);
        default:
          return (a, b) => b.created_at.unix() - a.created_at.unix();
      }
    };
    return sort.order === "desc" ? (a, b) => -sortFn(sort)(a, b) : sortFn(sort);
  }

  render() {
    let { sort, classes, selectedJobs, envCritical } = this.props;
    let { data } = this.state;

    let columns = ["name", "jobs", "period", "created", "created_by", "status", "detail"];
    let Data = () => {
      if (data && data.length) {
        const sortedData = [...data].sort(Backfills.sortFunction(sort));
        return (
          <Table
            envCritical={envCritical}
            columns={columns.map(column => {
              switch (column) {
                case "name":
                  return { id: "name", label: "Name", sortable: true };
                case "jobs":
                  return { id: "jobs", label: "Jobs", sortable: true };
                case "period":
                  return { id: "period", label: "Period", sortable: true };
                case "created":
                  return { id: "created", label: "Created", sortable: true };
                case "created_by":
                  return { id: "created_by", label: "Created By", sortable: true };
                case "status":
                  return {
                    id: "status",
                    label: "Status",
                    width: 120,
                    sortable: true
                  };
                case "detail":
                  return { id: "detail", width: 40 };
              }
            })}
            onSortBy={this.sortBy.bind(this)}
            sort={sort}
            data={sortedData}
            render={(
              column,
              { id, name, jobs, start, end, created_at, created_by, status }: Backfill
            ) => {
              switch (column) {
                case "name":
                  return <Link href={`/timeseries/backfills/${id}`}>{name}</Link>; //TODO detail screen + links repetition
                case "jobs":
                  return <span>{jobs.length}</span>;
                case "period":
                  return (
                    <TimeRangeLink
                      href={`/timeseries/calendar/${urlFormat(start)}_${urlFormat(end)}`}
                      start={start}
                      end={end}
                    />
                  );
                case "created":
                  return (
                    <Clock className={classes.time} time={created_at || ""} />
                  );
                case "created_by":
                  return <span>{created_by}</span>
                case "status":
                  return (
                    <Link
                      className={classes.openIcon}
                      href={`/timeseries/backfills/${id}`}
                    >
                      <BackfillStatus status={status} />
                    </Link>
                  );
                case "detail":
                  return (
                    <Link
                      className={classes.openIcon}
                      href={`/timeseries/backfills/${id}`}
                    >
                      <OpenIcon />
                    </Link>
                  );
              }
            }}
          />
        );
      } else if (data) {
        return (
          <div className={classes.noData}>
            No backfills
            {selectedJobs.length ? " (some may have been filtered)" : ""}
          </div>
        );
      } else {
        return <Spinner />;
      }
    };

    return (
      <div className={classes.container}>
        <h1 className={classes.title}>Backfills</h1>
        <PopoverMenu
          className={classes.menu}
          items={[
            <Link href="/timeseries/backfills/create">Create backfill</Link>
          ]}
        />
        <div className={classes.grid}>
          <div className={classes.data}><Data /></div>
        </div>
      </div>
    );
  }
}

// TODO duplicated styles
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
  menu: {
    position: "absolute",
    top: "1em",
    right: "1em"
  },
  grid: {
    overflow: "scroll",
    flex: "1",
    display: "flex",
    flexDirection: "column"
  },
  data: {
    display: "flex",
    flex: "1"
  },
  time: {
    color: "#8089a2"
  },
  noData: {
    flex: "1",
    textAlign: "center",
    fontSize: "0.9em",
    color: "#8089a2",
    alignSelf: "center",
    paddingBottom: "15%"
  },
  openIcon: {
    fontSize: "22px",
    color: "#607e96",
    padding: "15px",
    margin: "-15px"
  }
};

const mapStateToProps = ({
  app: { project, page: { sort, order }, statistics, selectedJobs }
}) => ({
  sort: {
    column: sort || "created",
    order: order || "asc"
  },
  statistics,
  selectedJobs,
  envCritical: project.env.critical
});
const mapDispatchToProps = dispatch => ({
  open(href, replace) {
    dispatch(navigate(href, replace));
  }
});

export default compose(
  injectSheet(styles),
  connect(mapStateToProps, mapDispatchToProps)
)(Backfills);
