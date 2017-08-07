// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";
import FancyTable from "../components/FancyTable";

import Window from "../components/Window";
import type {Backfill, ExecutionLog} from "../../datamodel";
import { listenEvents } from "../../Utils";
import Context from "../components/Context";
import JobStatus from "../components/JobStatus";
import { markdown } from "markdown";

import { BackfillsExecutions } from "./ExecutionLogs";

type Props = { 
  backfillId : string;
}
type State = {
  backfill : ?Backfill,
  query: ?string,
  eventSource: ?any,
  error: ?any,
  executionsEventSource: ?any,
  executions: Array<ExecutionLog>
}

class BackfillDetail extends React.Component {
  props : Props
  state : State
  constructor(props: Props) {
    super(props);
    this.state = {
      backfill : undefined,
      query: null,
      eventSource: null,
      error: null
    }
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

    const created = b =>`by ${b.created_by} on ${b.created}`;

    return (<Window 
      closeUrl={`/timeseries/backfills`}
      title="Backfill">
      {backfill ? 
      <FancyTable key="properties">
        <dt key="name">Name:</dt>
        <dd key="name_">{backfill.name}</dd>
        <dt key="jobs">Jobs:</dt>
        <dd key="jobs_">{backfill.jobs}</dd>
        <dt key="created">Created:</dt>
        <dd key="created_">{created(backfill)}</dd>
        <dt key="description">Description:</dt>
        {backfill.description ? <dd
          key="description_"
          dangerouslySetInnerHTML={{
            __html: markdown.toHTML(backfill.description)
          }}
        /> : null}
        <dt key="context">Context:</dt>
        <dd key="context_"><Context context={{start : backfill.start, end : backfill.end }} /></dd>
        <dt key="status">Status:</dt>
        <dd key="status_"><JobStatus status={backfill.status} /></dd>
      </FancyTable> : null}
      <BackfillsExecutions backfillId={this.props.backfillId} />
    </Window>)
  }

  componentWillUnmount() {
    let { eventSource } = this.state;
    eventSource && eventSource.close();
  }
}

export default injectSheet({})(BackfillDetail)