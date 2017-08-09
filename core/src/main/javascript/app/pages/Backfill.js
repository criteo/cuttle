// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";
import FancyTable from "../components/FancyTable";

import Window from "../components/Window";
import type { Backfill, ExecutionLog, Workflow } from "../../datamodel";
import { listenEvents } from "../../Utils";
import Context from "../components/Context";
import JobStatus from "../components/JobStatus";
import { markdown } from "markdown";
import Link from "../components/Link";

import { BackfillsExecutions } from "./ExecutionLogs";

type Props = {
  classes: any,
  backfillId: string,
  workflow: Workflow
};
type State = {
  backfill: ?Backfill,
  query: ?string,
  eventSource: ?any,
  error: ?any,
  executionsEventSource: ?any,
  executions: Array<ExecutionLog>,
  completion?: number;
};

class BackfillDetail extends React.Component {
  props: Props;
  state: State;
  constructor(props: Props) {
    super(props);
    this.state = {
      backfill: undefined,
      query: null,
      eventSource: null,
      error: null,
      completion : null
    };
  }

  componentWillMount() {
    this.listen();
  }

  listen() {
    let { query, eventSource } = this.state;
    let { backfillId } = this.props;
    let newQuery = `/api/timeseries/backfills/${backfillId}?events=true`;
    if (newQuery != query) {
      eventSource && eventSource.close();
      eventSource = listenEvents(
        newQuery,
        this.updateData.bind(this),
        this.notFound.bind(this)
      );
      this.setState({
        query: newQuery,
        backfill: null,
        eventSource
      });
    }
  }

  notFound(error) {
    this.setState({
      error
    });
  }

  updateData(json: Backfill) {
    this.setState({
      ...this.state,
      backfill: json
    });
  }

  render() {
    const { backfill, error } = this.state;
    const { classes } = this.props;

    const created = b => `by ${b.created_by} on ${b.created}`;

    const jobLinks =
      backfill &&
      backfill.jobs.split(",").map(jobId => {
        const jobName = this.props.workflow.jobName(jobId)
        return (
          <Link className="job-badge" href={`/workflow/${jobId}`}>
            <span>{jobName}</span>
          </Link>
        );
      });

    return (
      <div className={classes.main}>
        <Window closeUrl={`/timeseries/backfills`} title="Backfill">
          {backfill
            ? <FancyTable key="properties">
                <dt key="name">Name:</dt>
                <dd key="name_">{backfill.name}</dd>
                <dt key="jobs">Jobs:</dt>
                <dd key="jobs_">{jobLinks}</dd>
                <dt key="created">Created:</dt>
                <dd key="created_">{created(backfill)}</dd>
                <dt key="description">Description:</dt>
                <dd key="description_" style={{ lineHeight : 'inherit'}}
                    dangerouslySetInnerHTML={{
                      __html: markdown.toHTML(backfill.description)
                    }}/>
                <dt key="context">Context:</dt>
                <dd key="context_">
                  <Context
                    context={{ start: backfill.start, end: backfill.end }}
                  />
                </dd>
                <dt key="status">Status:</dt>
                <dd key="status_"><JobStatus status={backfill.status} />
                  <span className="backfill-completion">
                    {this.state.completion ? `${(this.state.completion*100).toFixed(2)} % completed`: null}
                  </span></dd>
              </FancyTable>
            : null}
          <BackfillsExecutions 
            backfillId={this.props.backfillId} 
            completionNotifier={(c) => this.setState({ completion : c})}/>
        </Window>
      </div>
    );
  }

  componentWillUnmount() {
    let { eventSource } = this.state;
    eventSource && eventSource.close();
  }
}

const mapStateToProps = ({ app: { workflow } }) => ({
  workflow
});

const styles = {
  main: {
    "& .job-badge": {
      display: "inline-block",
      backgroundColor: "#5E6A87",
      color: "#FFF",
      fontFamily: "Arial",
      fontWeight: "normal",
      lineHeight: "18px",
      fontSize: "11px",
      borderRadius: "2px",
      padding: "0 .5em",
      maxHeight: "18px",
      textAlign: "center",
      marginLeft: "5px"
    },
    "& .backfill-completion" : {
      marginLeft : "5px"
    }
  }
};

export default connect(mapStateToProps)(injectSheet(styles)(BackfillDetail));
