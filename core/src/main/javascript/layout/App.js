// @flow

import React from "react";
import { connect } from "react-redux";
import { navigate } from "redux-url";
import injectSheet from "react-jss";

import RightPane from "./RightPane";
import LeftPane from "./leftpane/LeftPane";
import MenuHeader from "./leftpane/MenuHeader";
import Menu from "./leftpane/Menu";
import type { PageId } from "../state";

import WorkflowContainer from "../tabs/WorkflowContainer";

type Props = {
  activeTab: PageId,
  workflowName: string,
  environment: string,
  classes: any
};

class App extends React.Component {
  props: Props;

  render() {
    const {
      classes,
      workflowName,
      environment,
      activeTab
    } = this.props;
    return (
      <div className={classes.main}>
        <RightPane className={classes.rightpane}>
          {(activeTab === "workflow" && <WorkflowContainer />) || <div />}
        </RightPane>
        <LeftPane className={classes.leftpane}>
          <MenuHeader environment={environment} workflowName={workflowName} />
          <Menu activeTab={activeTab} />
        </LeftPane>
      </div>
    );
  }
}

let styles = {
  leftpane: {
    position: "fixed",
    width: "20%"
  },
  rightpane: {
    paddingLeft: "20%",
    width: "80%",
    position: "absolute"
  },
  main: {
    backgroundColor: "#ECF1F5"
  }
};

const mapStateToProps = ({ page }) => ({ activeTab: page });
const mapDispatchToProps = () => ({});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(App)
);
