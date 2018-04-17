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
import PopoverMenu from "../components/PopoverMenu";

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
type Columns = "name" | "status" | "actions"
type JobStatus = "active" | "paused"

type Sort = {
  column: string,
  order: Order
};

type User = {
  userId: string
};

type Job = {
  id: string,
  name: string,
  status: JobStatus
};

type PausedJob = {
  id: string,
  user: User,
  date: string
};

type JobsOrder = (Job, Job) => number;

const columns: Array<{
  id: Columns,
  label?: string,
  sortable: boolean
}> = [
  { id: "name", label: "Name", sortable: true },
  { id: "status", label: "Status", sortable: true, width: 100 },
  { id: "actions", sortable: false, width: 40 }
];

const column2Comp = {
  name: ({ id, name }) => (
    <Link href={`/workflow/${id}?showDetail=true`}>{name}</Link>
  ),
  status: ({ status }) => (
    <Badge
      label={status}
      width={75}
      light={true}
      kind={status === "paused" ? "default" : "info"}
    />
  )
};

const jobMenu = ({
  job,
  status,
  classes,
  update
}: {
  job: string,
  status: string,
  classes: any,
  update: () => any
}) => {
  const menuItems =
    status === "paused"
      ? [
          <span
            onClick={() =>
              fetch(`/api/jobs/resume?jobs=${job}`, {
                method: "POST",
                credentials: "include"
              }).then(update)
            }
          >
            Resume
          </span>
        ]
      : [
          <span
            onClick={() =>
              fetch(`/api/jobs/pause?jobs=${job}`, {
                method: "POST",
                credentials: "include"
              }).then(update)
            }
          >
            Pause
          </span>
        ];

  return <PopoverMenu className={classes.menu} items={menuItems} />;
};

const sortQueryString = (column: string, order: Order) =>
  `?sort=${column}&order=${order}`;

const sortFunction: Sort => JobsOrder = (sort: Sort) => (a: Job, b: Job) => {
  const nameOrder = (a: Job, b: Job) => a.name.localeCompare(b.name);
  const statusOrder = (a: Job, b: Job) => a.status.localeCompare(b.status);
  const sortFn = (sort: Sort) => {
    switch (sort.column) {
      case "name":
        return nameOrder;
      case "status":
        return statusOrder;
      default:
        return nameOrder;
    }
  };

  return sort.order === "desc" ? -sortFn(sort)(a, b) : sortFn(sort)(a, b);
};

const processResponse = (response: Response) => {
  if (!response.ok) throw new Error({ _error: response.statusText });
  return response.json();
};

const fetchJobs = (persist: ({ data: Array<Job> }) => void) => {
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
    No jobs defined in your project
  </div>
);

class JobsComp extends React.Component<any, Props, State> {
  state: State;

  constructor(props: Props) {
    super(props);
    this.state = {
      data: null,
      pausedJobs: null
    };
  }

  componentDidMount() {
    const persist = this.setState.bind(this);
    fetchJobs(persist);
    fetchPausedJobs(persist);
  }

  componentWillReceiveProps() {
    this.componentDidMount();
  }

  render() {
    const { classes, status, sort, selectedJobs, envCritical } = this.props;
    const { data, pausedJobs } = this.state;
    const setOfSelectedJobs = new Set(selectedJobs);
    const update = () => fetchPausedJobs(this.setState.bind(this));
    const Data = () => {
      if (data && data.length && pausedJobs) {
        const jobs = data.map((job: Job) => {
          return {
            ...job,
            status: pausedJobs.has(job.id) ? "paused" : "active"
          };
        });
        const preparedData = jobs
          .filter(
            job =>
              (setOfSelectedJobs.size === 0 || setOfSelectedJobs.has(job.id))
          )
          .sort(sortFunction(sort));

        return preparedData.length !== 0 ? (
          <Table
            data={preparedData}
            columns={columns}
            sort={sort}
            envCritical={envCritical}
            onSortBy={this.handleSortBy.bind(this)}
            render={(column, row) => {
              switch (column) {
                case "actions":
                  return jobMenu({ update, job: row.id, status: row.status, classes });
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
        <h1 className={classes.title}>All jobs in the project</h1>
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
  { app: { project, page: { sort, order }, selectedJobs } },
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
