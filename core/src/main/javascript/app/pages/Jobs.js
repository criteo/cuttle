// @flow

import * as React from "react";
import injectSheet from "react-jss";
import { connect } from "react-redux";
import { compose } from "redux";
import { navigate } from "redux-url";
import { displayFormat } from "../utils/Date";
import Spinner from "../components/Spinner";
import Table from "../components/Table";
import { Badge } from "../components/Badge";

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
  pausedJobs: Set<string> | null
};

type Order = "asc" | "desc";

type Sort = {
  column: string,
  order: Order
};

type Columns = "id" | "name" | "description";

type Scheduling = {
  start: string
};

type JobStatus = "Paused" | "Active";

type Job = {
  [Columns]: string,
  scheduling: Scheduling,
  status: JobStatus
};

type JobsOrder = (Job, Job) => number;

const columns: Array<{
  id: Columns | "startDate" | "status",
  label: string,
  sortable: boolean
}> = [
  { id: "id", label: "ID", sortable: true },
  { id: "name", label: "Name", sortable: true },
  { id: "description", label: "Description", sortable: false },
  { id: "startDate", label: "Start Date", sortable: true },
  { id: "status", label: "Status", sortable: true }
];

const column2Comp: {
  [Columns]: ({ [Columns]: string }) => any,
  startDate: ({ scheduling: Scheduling }) => any,
  status: ({ status: JobStatus }) => any
} = {
  id: ({ id }: { id: string }) => <span>{id}</span>,
  name: ({ name }: { name: string }) => <span>{name}</span>,
  description: ({ description }: { description: string }) => (
    <span>{description}</span>
  ),
  startDate: ({ scheduling }: { scheduling: Scheduling }) => (
    <span>{displayFormat(new Date(scheduling.start))}</span>
  ),
  status: ({ status }: { status: JobStatus }) => (
    <Badge
      label={status}
      width={75}
      light={true}
      kind={status === "Paused" ? "default" : "info"}
    />
  )
};

const sortQueryString = (column: string, order: Order) =>
  `?sort=${column}&order=${order}`;

const sortFunction: Sort => JobsOrder = (sort: Sort) => (a: Job, b: Job) => {
  const idOrder = (a: Job, b: Job) => a.id.localeCompare(b.id);
  const nameOrder = (a: Job, b: Job) => a.name.localeCompare(b.name);
  const statusOrder = (a: Job, b: Job) => a.status.localeCompare(b.status);
  const sortFn = (sort: Sort) => {
    switch (sort.column) {
      case "id":
        return idOrder;
      case "name":
        return nameOrder;
      case "status":
        return statusOrder;
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
  return fetch("/api/workflow_definition")
    .then(processResponse)
    .then(json => ({ data: json.jobs }))
    .then(persist);
};

const fetchPausedJobs = (persist: ({ pausedJobs: Set<string> }) => void) => {
  return fetch("/api/jobs/paused")
    .then(processResponse)
    .then(jsonArr => ({ pausedJobs: new Set(jsonArr) }))
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
    {`No ${status.toLocaleLowerCase()} jobs for now`}
    {selectedJobs.length ? " (some may have been filtered)" : ""}
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
    fetchWorkflow(persist);
    fetchPausedJobs(persist);
  }

  componentWillReceiveProps() {
    this.componentDidMount();
  }

  render() {
    const { classes, status, sort, selectedJobs, envCritical } = this.props;
    const { data, pausedJobs } = this.state;
    const setOfSelectedJobs = new Set(selectedJobs);

    const Data = () => {
      if (data && data.length && pausedJobs) {
        const preparedData = data
          .map(datum => ({
            ...datum,
            status: pausedJobs.has(datum.id) ? "Paused" : "Active"
          }))
          .filter(
            job =>
              (setOfSelectedJobs.size === 0 || setOfSelectedJobs.has(job.id)) &&
              (job.status === status || status === "All")
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
              return column2Comp[column](row);
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
