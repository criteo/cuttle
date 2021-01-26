// @flow

import * as React from "react";
import injectSheet from "react-jss";
import { connect } from "react-redux";
import { compose } from "redux";
import { navigate } from "redux-url";
import { displayFormat } from "../../common/utils/Date";
import ResumeIcon from "react-icons/lib/md/play-arrow";
import PauseIcon from "react-icons/lib/md/pause";
import Table from "../../common/components/Table";
import Spinner from "../../common/components/Spinner";
import { Badge } from "../../common/components/Badge";
import PopoverMenu from "../../common/components/PopoverMenu";
import isEqual from "lodash/isEqual";
import classNames from "classnames";
import type {
  Dag,
  DagScheduling,
  DagState,
  DagStatus,
  Workflow
} from "../datamodel";

type Props = {
  classes: any,
  status: string,
  workflow: Workflow,
  selectedDags: Array<string>,
  envCritical: boolean,
  sort: Sort,
  open: (link: string, replace: boolean) => void
};

type State = {
  dagStates: ?(DagState[])
};

type Order = "asc" | "desc";

type Sort = {
  column: string,
  order: Order
};

type Columns = "id" | "name";

type DisplayedDag = {
  [Columns]: string,
  scheduling: DagScheduling,
  status: DagStatus,
  nextInstant: string,
  pausedUser: string,
  pausedDate: string
};

type DagsOrder = (DisplayedDag, DisplayedDag) => number;

const columns: Array<{
  id:
    | Columns
    | "expression"
    | "tz"
    | "status"
    | "nextInstant"
    | "pausedUser"
    | "pausedDate"
    | "pauseAction"
    | "runAction",
  label?: string,
  sortable: boolean
}> = [
  { id: "id", label: "ID", sortable: true },
  { id: "name", label: "Name", sortable: true },
  { id: "expression", label: "Scheduling", sortable: true },
  { id: "tz", label: "Timezone", sortable: true },
  { id: "status", label: "Status", sortable: true },
  { id: "nextInstant", label: "Next Run At", sortable: true },
  { id: "pausedUser", label: "Paused By", sortable: true },
  { id: "pausedDate", label: "Paused At", sortable: true },
  { id: "pauseAction", sortable: false, width: 40 },
  { id: "runAction", sortable: false, width: 40 }
];

const column2Comp: {
  [Columns]: ({ [Columns]: string }) => any,
  expression: ({ scheduling: DagScheduling }) => any,
  tz: ({ scheduling: DagScheduling }) => any,
  status: ({ status: DagStatus }) => any,
  nextInstant: ({ nextInstant: string }) => any,
  pausedUser: ({ pausedUser: string }) => any,
  pausedDate: ({ pausedDate: string }) => any
} = {
  id: ({ id }: { id: string }) => <span>{id}</span>,
  name: ({ name }: { name: string }) => <span>{name}</span>,
  expression: ({ scheduling }: { scheduling: DagScheduling }) => (
    <span>{scheduling.expression}</span>
  ),
  tz: ({ scheduling }: { scheduling: DagScheduling }) => (
    <span>{scheduling.tz}</span>
  ),
  status: ({ status }: { status: DagStatus }) => (
    <Badge
      label={status}
      width={75}
      light={true}
      kind={
        status === "paused"
          ? "default"
          : status === "waiting"
          ? "warning"
          : "info"
      }
    />
  ),
  nextInstant: ({ nextInstant }: { nextInstant: string }) => (
    <span>{nextInstant ? displayFormat(new Date(nextInstant)) : ""}</span>
  ),
  pausedUser: ({ pausedUser }: { pausedUser: string }) => (
    <span>{pausedUser}</span>
  ),
  pausedDate: ({ pausedDate }: { pausedDate: string }) => (
    <span>{pausedDate ? displayFormat(new Date(pausedDate)) : ""}</span>
  )
};

const sortQueryString = (column: string, order: Order) =>
  `?sort=${column}&order=${order}`;

const sortFunction: Sort => DagsOrder = (sort: Sort) => (
  a: DisplayedDag,
  b: DisplayedDag
) => {
  const idOrder = (a: DisplayedDag, b: DisplayedDag) =>
    a.id.localeCompare(b.id);
  const nameOrder = (a: DisplayedDag, b: DisplayedDag) =>
    a.name.localeCompare(b.name);
  const statusOrder = (a: DisplayedDag, b: DisplayedDag) =>
    a.status.localeCompare(b.status);
  const userOrder = (a: DisplayedDag, b: DisplayedDag) =>
    a.pausedUser.localeCompare(b.pausedUser);
  const nextInstantOrder = (a: DisplayedDag, b: DisplayedDag) =>
    formatInstantForCompare(a.nextInstant).localeCompare(
      formatInstantForCompare(b.nextInstant)
    );
  const pausedDateOrder = (a: DisplayedDag, b: DisplayedDag) =>
    formatInstantForCompare(a.pausedDate).localeCompare(
      formatInstantForCompare(b.pausedDate)
    );
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
      case "nextInstant":
        return nextInstantOrder;
      case "pausedDate":
        return pausedDateOrder;
      default:
        return idOrder;
    }
  };

  return sort.order === "desc" ? -sortFn(sort)(a, b) : sortFn(sort)(a, b);
};

const formatInstantForCompare = (str: string) => {
  return (str && new Date(str).toISOString()) || str;
};

const processResponse = (response: Response) => {
  if (!response.ok) throw new Error({ _error: response.statusText });
  return response.json();
};

const fetchDagStates = (
  persist: ({ dagStates: DagState[] }) => void,
  dags: string[]
) => {
  return fetch("/api/dags/states", {
    method: "POST",
    body: JSON.stringify({ dags: dags })
  })
    .then(processResponse)
    .then((jsonArr: DagState[]) => {
      return { dagStates: jsonArr };
    })
    .then(persist);
};

const dagAction = (
  action: string,
  dags: string[],
  selectedDags: string[],
  persist: ({ dagStates: DagState[] }) => void
) => {
  return () => {
    fetch(`/api/dags/${action}`, {
      method: "POST",
      credentials: "include",
      body: JSON.stringify({ dags: dags })
    }).then(() => fetchDagStates(persist, selectedDags));
  };
};

const NoDags = ({
  className,
  status,
  selectedDags
}: {
  className: string,
  status: string,
  selectedDags: Array<string>
}) => (
  <div className={className}>
    {`No ${status.toLocaleLowerCase()} dags for now`}
    {selectedDags.length ? " (some may have been filtered)" : ""}
  </div>
);

class DagsComp extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      dagStates: null
    };
  }

  componentDidMount() {
    const { selectedDags } = this.props;
    const persist = this.setState.bind(this);
    fetchDagStates(persist, selectedDags);
  }

  componentWillReceiveProps() {
    const { selectedDags } = this.props;
    const persist = this.setState.bind(this);
    fetchDagStates(persist, selectedDags);
  }

  shouldComponentUpdate(nextProps, nextState) {
    return (
      !isEqual(this.props, nextProps) ||
      !isEqual(this.state.dagStates, nextState.dagStates)
    );
  }

  render() {
    const {
      classes,
      status,
      sort,
      selectedDags,
      workflow,
      envCritical
    } = this.props;
    const { dagStates } = this.state;
    const setOfSelectedDags = new Set(selectedDags);
    const Data = () => {
      if (workflow.dags.length && dagStates) {
        const visibleColumns = columns.filter(
          column =>
            status === "all" ||
            (status === "active" &&
              column.id !== "pausedDate" &&
              column.id !== "pausedUser") ||
            (status === "paused" &&
              column.id !== "nextInstant" &&
              column.id !== "runAction")
        );
        const dags: Array<DisplayedDag> = workflow.dags.map((dag: Dag) => {
          const state: DagState = dagStates.find(
            state => state.id == dag.id
          ) || { id: dag.id, status: "running" };
          return {
            id: dag.id,
            name: dag.name,
            scheduling: dag.expression,
            status: state.status,
            nextInstant: state.nextInstant || "",
            pausedUser: state.pausedUser || "",
            pausedDate: state.pausedDate || ""
          };
        });
        const preparedData = dags
          .filter(
            dag =>
              (setOfSelectedDags.size === 0 || setOfSelectedDags.has(dag.id)) &&
              (dag.status === status ||
                status === "all" ||
                (dag.status !== "paused" && status === "active"))
          )
          .sort(sortFunction(sort));

        const persist = this.setState.bind(this);
        return preparedData.length !== 0 ? (
          <Table
            data={preparedData}
            columns={visibleColumns}
            sort={sort}
            envCritical={envCritical}
            onSortBy={this.handleSortBy.bind(this)}
            render={(column, row) => {
              switch (column) {
                case "pauseAction":
                  return row.status === "paused" ? (
                    <a
                      onClick={dagAction(
                        "resume",
                        [row.id],
                        selectedDags,
                        persist
                      )}
                      className={classNames(classes.link, classes.actionIcon)}
                      title="Resume"
                    >
                      <ResumeIcon />
                    </a>
                  ) : (
                    <a
                      onClick={dagAction(
                        "pause",
                        [row.id],
                        selectedDags,
                        persist
                      )}
                      className={classNames(classes.link, classes.actionIcon)}
                      title="Pause"
                    >
                      <PauseIcon />
                    </a>
                  );
                case "runAction":
                  return row.status !== "waiting" ? (
                    ""
                  ) : (
                    <PopoverMenu
                      className={classes.menu}
                      items={[
                        <span
                          onClick={dagAction(
                            "runnow",
                            [row.id],
                            selectedDags,
                            persist
                          )}
                        >
                          {`Run '${row.name}' Now`}
                        </span>
                      ]}
                    />
                  );
                default:
                  return column2Comp[column](row);
              }
            }}
          />
        ) : (
          <NoDags
            className={classes.noData}
            status={status}
            selectedDags={selectedDags}
          />
        );
      } else if (dagStates !== null) {
        return (
          <NoDags
            className={classes.noData}
            status={status}
            selectedDags={selectedDags}
          />
        );
      } else {
        return <Spinner />;
      }
    };

    const persist = this.setState.bind(this);
    const pause = dagAction("pause", selectedDags, selectedDags, persist);
    const resume = dagAction("resume", selectedDags, selectedDags, persist);
    const isFilterApplied = selectedDags.length > 0;
    const menuItems = [];
    if (isFilterApplied) {
      menuItems.push(
        <span onClick={pause}>Pause {selectedDags.length} filtered dags</span>,
        <span onClick={resume}>Resume {selectedDags.length} filtered dags</span>
      );
    } else {
      menuItems.push(
        <span onClick={pause}>Pause everything</span>,
        <span onClick={resume}>Resume everything</span>
      );
    }

    return (
      <div className={classes.container}>
        <h1 className={classes.title}>Job DAGs</h1>
        <PopoverMenu className={classes.menu} items={menuItems} />
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
  },
  actionIcon: {
    cursor: "pointer",
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
      selectedJobs,
      workflow
    }
  },
  ownProps
) => {
  return {
    sort: {
      column: sort || "id",
      order: order || "asc"
    },
    selectedDags: workflow.dags
      .filter(dag =>
        dag.pipeline.vertices.some(job => selectedJobs.includes(job))
      )
      .map(dag => dag.id),
    envCritical: project.env.critical,
    status: ownProps.status,
    workflow
  };
};

const mapDispatchToProps = dispatch => ({
  open(href, replace) {
    dispatch(navigate(href, replace));
  }
});

export const Dags = compose(
  injectSheet(styles),
  connect(
    mapStateToProps,
    mapDispatchToProps
  )
)(DagsComp);
