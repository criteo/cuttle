// @flow

import React from "react";
import { connect } from "react-redux";

import WorkflowComponent from "../components/tabs/Workflow";
import type { Workflow } from "../datamodel/workflow";
import * as Actions from "../actions";

type Props = {
  workflow: Workflow,
  isLoadingWorkflow: boolean,
  isWorkflowLoaded: boolean,
  loadWorkflowData: () => void
};

class WorkflowContainer extends React.Component {
  props: Props;

  constructor(props: Props) {
    super(props);
    if (!this.props.isWorkflowLoaded && !this.props.isLoadingWorkflow)
      this.props.loadWorkflowData();
  }

  render() {
    const { workflow, isLoadingWorkflow } = this.props;
    return (
      <WorkflowComponent
        workflow={workflow}
        isLoadingWorkflow={isLoadingWorkflow}
      />
    );
  }
}

const mapStateToProps = ({ workflow = {} }) => ({
  isLoadingWorkflow: workflow.isLoading,
  isWorkflowLoaded: !!workflow.data,
  workflow: workflow.data
});

const mapDispatchToProps = dispatch => ({
  loadWorkflowData: () => Actions.loadWorkflowData(dispatch)
});

export default connect(mapStateToProps, mapDispatchToProps)(WorkflowContainer);
