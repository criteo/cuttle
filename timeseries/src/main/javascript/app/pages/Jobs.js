// @flow

import * as React from "react";
import injectSheet from "react-jss";
import { connect } from "react-redux";
import { compose } from "redux";
import { navigate } from "redux-url";
import { displayFormat } from "../utils/Date";
import OpenIcon from "react-icons/lib/md/zoom-in";
import Spinner from "../components/Spinner";
import Table from "../components/Table";
import Link from "../components/Link";
import { Badge } from "../components/Badge";
import type { JobStatus } from "../../ApplicationState";
import PopoverMenu from "../components/PopoverMenu";
import isEqual from "lodash/isEqual";

type Props = {
  classes: any,
  status: string,
  selectedJobs: Array<string>,
  envCritical: boolean,
  sort: Sort,
  open: (link: string, replace: boolean) => void
};

type State = {
  data: Array<Job> | null,
  pausedJobs: Map<string, PausedJob> | null
};

type Order = "asc" | "desc";

type Sort = {
  column: string,
  order: Order
};

type Columns = "id" | "name" | "date";

type Scheduling = {
  start: string
};

type User = {
  userId: string
};

type Job = {
  [Columns]: string,
  name: string,
  scheduling: Scheduling,
  status: JobStatus,
  user: User
};

type PausedJob = {
  id: string,
  user: User,
  date: string
};

type JobsOrder = (Job, Job) => number;

const columns: Array<{
  id: Columns | "startDate" | "status" | "user" | "detail" | "actions",
  label?: string,
  sortable: boolean
}> = [
  { id: "id", label: "ID", sortable: true },
  { id: "name", label: "Name", sortable: true },
  { id: "startDate", label: "Start Date", sortable: true },
  { id: "status", label: "Status", sortable: true },
  { id: "user", label: "Paused By", sortable: true },
  { id: "date", label: "Paused At", sortable: true },
  { id: "detail", sortable: false, width: 40 },
  { id: "actions", sortable: false, width: 40 }
];

const column2Comp: {
  [Columns]: ({ [Columns]: string }) => any,
  startDate: ({ scheduling: Scheduling }) => any,
  status: ({ status: JobStatus }) => any,
  user: ({ user: User }) => any,
  date: ({ date: string }) => any
} = {
  id: ({ id }: { id: string }) => <Link href={`/workflow/${id}`}>{id}</Link>,
  name: ({ name }: { name: string }) => <span>{name}</span>,
  startDate: ({ scheduling }: { scheduling: Scheduling }) => (
    <span>{displayFormat(new Date(scheduling.start))}</span>
  ),
  status: ({ status }: { status: JobStatus }) => (
    <Badge
      label={status}
      width={75}
      light={true}
      kind={status === "paused" ? "default" : "info"}
    />
  ),
  user: ({ user }: { user: User }) => <span>{user.userId}</span>,
  date: ({ date }: { date: string }) => (
    <span>{date ? displayFormat(new Date(date)) : ""}</span>
  )
};

const sortQueryString = (column: string, order: Order) =>
  `?sort=${column}&order=${order}`;

const sortFunction: Sort => JobsOrder = (sort: Sort) => (a: Job, b: Job) => {
  const idOrder = (a: Job, b: Job) => a.id.localeCompare(b.id);
  const nameOrder = (a: Job, b: Job) => a.name.localeCompare(b.name);
  const statusOrder = (a: Job, b: Job) => a.status.localeCompare(b.status);
  const userOrder = (a: Job, b: Job) =>
    a.user.userId.localeCompare(b.user.userId);
  const sortFn = (sort: Sort) => {
    switch (sort.column) {
      case "id":
        return idOrder;
      case "name":
        return nameOrder;
      case "status":
        return statusOrder;
      case "user":
        return userOrder;
      default:
        return idOrder;
    }
  };

  return sort.order === "desc" ? -sortFn(sort)(a, b) : sortFn(sort)(a, b);
};

const processResponse = (response: Response) => {
  if (!response.ok) throw new Error({ _error: response.statusText });
  return response.json();
};

const fetchWorkflow = (persist: ({ data: Array<Job> }) => void) => {
  return fetch("/api/jobs_definition")
    .then(processResponse)
    .then(json => ({ data: json.jobs }))
    .then(persist);
};

const fetchPausedJobs = (
  persist: ({ pausedJobs: Map<string, PausedJob> }) => void
) => {
  return fetch("/api/jobs/paused")
    .then(processResponse)
    .then((jsonArr: Array<PausedJob>) => {
      const pausedJobs = new Map();
      jsonArr.forEach(pausedJob => {
        pausedJobs.set(pausedJob.id, pausedJob);
      });
      return { pausedJobs: pausedJobs };
    })
    .then(persist);
};

const jobAction = (action, job, persist) => {
  return fetch(`/api/jobs/${action}?jobs=${job}`, {
    method: "POST",
    credentials: "include"
  }).then(() => fetchPausedJobs(persist));
};

const NoJobs = ({
  className,
  status,
  selectedJobs
}: {
  className: string,
  status: string,
  selectedJobs: Array<string>
}) => (
  <div className={className}>
    {`No ${status.toLocaleLowerCase()} jobs for now`}
    {selectedJobs.length ? " (some may have been filtered)" : ""}
  </div>
);

const activeJobsProps = {
  user: {
    userId: ""
  },
  date: "",
  status: "active"
};

const jobMenu = ({
  persist,
  job,
  status,
  classes
}: {
  persist: any,
  job: string,
  status: string,
  classes: any
}) => {
  const menuItems =
    status === "paused"
      ? [<span onClick={() => jobAction("resume", job, persist)}>Resume</span>]
      : [<span onClick={() => jobAction("pause", job, persist)}>Pause</span>];

  return <PopoverMenu className={classes.menu} items={menuItems} />;
};

class JobsComp extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      data: null,
      pausedJobs: null
    };
  }

  componentDidMount() {
    const persist = this.setState.bind(this);
    fetchWorkflow(persist);
    fetchPausedJobs(persist);
  }

  componentWillReceiveProps() {
    const persist = this.setState.bind(this);
    fetchPausedJobs(persist);
  }

  shouldComponentUpdate(nextProps, nextState) {
    return !isEqual(this.props, nextProps) ||
        !isEqual(this.state.data, nextState.data) ||
        !isEqual(this.state.pausedJobs, nextState.pausedJobs);
  }

  render() {
    const { classes, status, sort, selectedJobs, envCritical } = this.props;
    const { data, pausedJobs } = this.state;
    const setOfSelectedJobs = new Set(selectedJobs);
    const Data = () => {
      if (data && data.length && pausedJobs) {
        const jobs = data.map((job: Job) =>
          Object.assign(
            {},
            job,
            pausedJobs.has(job.id)
              ? Object.assign({}, pausedJobs.get(job.id), { status: "paused" })
              : activeJobsProps
          )
        );
        const preparedData = jobs
          .filter(
            job =>
              (setOfSelectedJobs.size === 0 || setOfSelectedJobs.has(job.id)) &&
              (job.status === status || status === "all")
          )
          .sort(sortFunction(sort));

        const persist = this.setState.bind(this);
        return preparedData.length !== 0 ? (
          <Table
            data={preparedData}
            columns={columns}
            sort={sort}
            envCritical={envCritical}
            onSortBy={this.handleSortBy.bind(this)}
            render={(column, row) => {
              switch (column) {
                case "detail":
                  return (
                    <Link
                      className={classes.openIcon}
                      href={`/workflow/${row.id}?showDetail=true&refPath=${
                        location.pathname
                      }`}
                    >
                      <OpenIcon />
                    </Link>
                  );
                case "actions":
                  return jobMenu({
                    persist: persist,
                    job: row.id,
                    status: row.status,
                    classes
                  });
                default:
                  return column2Comp[column](row);
              }
            }}
          />
        ) : (
          <NoJobs
            className={classes.noData}
            status={status}
            selectedJobs={selectedJobs}
          />
        );
      } else if (data && pausedJobs) {
        return (
          <NoJobs
            className={classes.noData}
            status={status}
            selectedJobs={selectedJobs}
          />
        );
      } else {
        return <Spinner />;
      }
    };

    return (
      <div className={classes.container}>
        <h1 className={classes.title}>Jobs</h1>
        <div className={classes.grid}>
          <div className={classes.data}>
            <Data />
          </div>
        </div>
      </div>
    );
  }

  handleSortBy(column: string) {
    const { sort } = this.props;
    const order = sort.order === "asc" ? "desc" : "asc";
    this.props.open(sortQueryString(column, order), false);
  }
}

const styles = {
  container: {
    padding: "1em",
    flex: "1",
    display: "flex",
    flexDirection: "column",
    position: "relative",
    minHeight: "0"
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
    overflow: "auto",
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

const mapStateToProps = (
  {
    app: {
      project,
      page: { sort, order },
      selectedJobs
    }
  },
  ownProps
) => {
  return {
    sort: {
      column: sort || "id",
      order: order || "asc"
    },
    selectedJobs,
    envCritical: project.env.critical,
    status: ownProps.status
  };
};

const mapDispatchToProps = dispatch => ({
  open(href, replace) {
    dispatch(navigate(href, replace));
  }
});

export const Jobs = compose(
  injectSheet(styles),
  connect(mapStateToProps, mapDispatchToProps)
)(JobsComp);
