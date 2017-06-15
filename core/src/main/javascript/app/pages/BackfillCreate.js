// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";
import { goBack } from "redux-url";
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
          `/api/timeseries/backfill?name=${name}&job=${job}&startDate=${dateFormat(start)}&endDate=${dateFormat(end)}&priority=${priority}`,
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
        <div className={classes.filter}>
          <JobSelector
            workflow={workflow}
            selected={selectedJobs}
            onChange={this.selectJobs}
          />
        </div>
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
  filter: {
    margin: "-1em -1em 1em -1em", //TODO that is fishy ;(
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
