// @flow

import React from "react";
import { connect } from "react-redux";

import WorkflowComponent from "../components/tabs/Workflow";
import type { Workflow } from "../datamodel/workflow";
import * as Actions from "../actions";

type Props = {
  workflow: Workflow,
  loadWorkflowData: () => void
};

class WorkflowContainer extends React.Component {
  props: Props;

  constructor(props: Props) {
    super(props);
    this.props.loadWorkflowData();
  }

  render() {
    const { workflow } = this.props;
    return workflow
      ? <WorkflowComponent workflow={workflow} />
      : <div />;
  }
}

const mapStateToProps = ({ workflow }) => ({ workflow });

const mapDispatchToProps = dispatch => ({
  loadWorkflowData: () => Actions.loadWorkflowData(dispatch)
});

export default connect(mapStateToProps, mapDispatchToProps)(WorkflowContainer);
