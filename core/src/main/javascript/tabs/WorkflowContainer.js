// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";

type Props = {
  classes: any
};

class WorkflowContainer extends React.Component {
  props: Props;

  render() {
    const { classes } = this.props;
    return (
      <div className={classes.main}>
        <h1>Workflow Definition</h1>
      </div>
    );
  }
}

let styles = {
  main: {
    backgroundColor: "#ECF1F5"
  }
};

const mapStateToProps = () => ({});

const mapDispatchToProps = () => ({});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(WorkflowContainer)
);
