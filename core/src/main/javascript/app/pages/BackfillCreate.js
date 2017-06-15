// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";
import goBack from "redux-url";
import CloseIcon from "react-icons/lib/md/close";
import moment from "moment";

import Window from "../components/Window";
import FancyTable from "../components/FancyTable";
import JobSelector from "../components/JobSelector";
import type { Workflow } from "../../datamodel";

type Props = {
  workflow: ?Workflow,
  classes: any,
  back: () => void
};

type State = {
  selectedJobs: Array<string>,
  name: string,
  start: Date,
  end: Date,
  priority: number
};

class BackfillCreate extends React.Component {
  props: Props;
  state: State;

  constructor(props: Props) {
    super(props);
    (this: any).handleInputChange = this.handleInputChange.bind(this);
    (this: any).createBackfill = this.createBackfill.bind(this);
    (this: any).selectJobs = this.selectJobs.bind(this);
    this.state = {
      selectedJobs: [],
      name: "",
      start: moment({ hour: 0 }),
      end: moment({ hour: 1 }),
      priority: 0
    };
  }

  handleInputChange(event) {
    const target = event.target;
    const value = target.type === "checkbox" ? target.checked : target.value;
    const name = target.name;
    this.setState({ [name]: value });
  }

  selectJobs(jobs: Array<string>) {
    this.setState({ selectedJobs: jobs });
  }

  createBackfill(e) {
    e.preventDefault();
    const { selectedJobs, name, start, end, priority } = this.state;
    const dateFormat = date => {
      return moment.utc(date).toISOString();
    };
    return Promise.all(
      selectedJobs.map(job =>
        fetch(
          `/api/timeseries/backfill?job=${job}&startDate=${dateFormat(start)}&endDate=${dateFormat(end)}&priority=${priority}`,
          { method: "POST" }
        )
      )
    );
  }

  render() {
    let { classes, workflow, back } = this.props;
    let { selectedJobs } = this.state;
    const dateFormat = date => {
      return moment(date).format("YYYY-MM-DDTHH:mm");
    };

    return (
      <Window title="Create Backfill">
        <CloseIcon className={classes.close} onClick={back} />
        <form onSubmit={this.createBackfill}>
          <FancyTable key="properties">
            <dt key="name_">Name:</dt>
            <dd key="name">
              <input
                name="name"
                type="text"
                value={this.state.name}
                onChange={this.handleInputChange}
                required
              />
            </dd>
            <dt key="start_">Start:</dt>
            <dd key="start">
              <input
                name="start"
                type="datetime-local"
                value={dateFormat(this.state.start)}
                onChange={this.handleInputChange}
                required
              />
            </dd>
            <dt key="end_">End:</dt>
            <dd key="end">
              <input
                name="end"
                type="datetime-local"
                value={dateFormat(this.state.end)}
                onChange={this.handleInputChange}
                required
              />
            </dd>
            <dt key="priority_">Priority:</dt>
            <dd key="priority">
              <input
                name="priority"
                type="number"
                value={this.state.priority}
                onChange={this.handleInputChange}
                required
              />
            </dd>
            <dt key="create_" />
            <dd key="create"><button>Create</button></dd>
          </FancyTable>
        </form>
        <div className={classes.mainFilter}>
          <JobSelector
            workflow={workflow}
            selected={selectedJobs}
            placeholder={
              <span>
                {/*<FilterIcon className={classes.filterIcon}/>*/}
                Filter on specific jobs...
              </span>
            }
            onChange={this.selectJobs}
          />
        </div>
      </Window>
    );
  }
}

// TODO duplicated styles
const styles = {
  close: {
    position: "absolute",
    color: "#eef5fb",
    top: ".75em",
    right: ".5em",
    cursor: "pointer",
    fontSize: "20px"
  },
  definitions: {
    margin: "-1em",
    display: "flex",
    flexFlow: "row",
    flexWrap: "wrap",
    fontSize: ".85em",
    background: "rgba(189, 213, 228, 0.1)",
    "& dt": {
      flex: "0 0 150px",
      textOverflow: "ellipsis",
      overflow: "hidden",
      padding: "0 1em",
      boxSizing: "border-box",
      textAlign: "right",
      color: "#637686",
      lineHeight: "2.75em"
    },
    "& dd": {
      flex: "0 0 calc(100% - 150px)",
      marginLeft: "auto",
      textAlign: "left",
      textOverflow: "ellipsis",
      overflow: "hidden",
      padding: "0",
      boxSizing: "border-box",
      lineHeight: "2.75em"
    },
    "& dd:nth-of-type(even), & dt:nth-of-type(even)": {
      background: "#eef5fb"
    }
  },
  failedLink: {
    color: "#e91e63"
  },
  streams: {
    flex: "1",
    display: "flex",
    background: "#23252f",
    margin: "1em -1em -1em -1em",
    position: "relative",

    "& ul": {
      flex: "1",
      overflow: "scroll",
      padding: "1em",
      margin: "0",
      listStyle: "none",
      fontSize: ".85em",
      lineHeight: "1.5em"
    },

    "& span": {
      width: "150px",
      color: "#747a88",
      display: "inline-block",
      marginRight: "-10px",
      boxSizing: "border-box"
    },

    "& p": {
      display: "inline-block",
      margin: "0",
      color: "#f1f1f1",
      whiteSpace: "pre"
    }
  },
  fullscreen: {
    position: "fixed",
    top: "0",
    left: "0",
    right: "0",
    bottom: "0",
    margin: "0",
    zIndex: "99999"
  },
  fullscreenButton: {
    cursor: "pointer",
    color: "#fff",
    fontSize: "22px",
    position: "absolute",
    right: "10px",
    top: "10px"
  },
  autoScrollButton: {
    cursor: "pointer",
    color: "#fff",
    fontSize: "22px",
    position: "absolute",
    right: "36px",
    top: "10px"
  },
  activeAutoScroll: {
    color: "#66cb63"
  },
  DEBUG: {
    color: "#FFFF91 !important"
  },
  ERROR: {
    color: "#FF6C60 !important"
  },
  // from App.js
  mainFilter: {
    zIndex: "2",
    background: "#fff",
    height: "4em",
    lineHeight: "4em",
    boxShadow: "0px 1px 5px 0px #BECBD6"
  }
};

const mapStateToProps = ({ workflow }) => ({
  workflow
});
const mapDispatchToProps = dispatch => ({
  back() {
    dispatch(goBack());
  }
});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(BackfillCreate)
);
