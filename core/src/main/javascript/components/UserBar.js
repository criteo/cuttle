// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import { connect } from "react-redux";
import * as Actions from "../actions";
import React from "react";
import { Collapse } from "react-collapse";
import { presets } from "react-motion";
import { Job } from "../datamodel/workflow";

import map from "lodash/map";

type Props = {
  classes: any,
  className: any,
  children: any,
  selectedJobs: string[],
  allJobs: { [string]: Job },
  toggleUserbar: () => void,
  deselectJob: () => void,
  open: boolean
};

class UserBar extends React.Component {
  props: Props;
  state: {
    focus: boolean
  };

  constructor(props: Props) {
    super(props);
    this.state = {
      focus: false
    };
  }

  focus() {
    this.setState({ focus: true });
  }

  blur() {
    this.setState({ focus: false });
  }

  render() {
    const { focus } = this.state;
    const { classes, className, children, allJobs, selectedJobs, open, toggleUserbar, deselectJob }: Props = this.props;
    return (
      <div
        className={classNames(className, classes.bar, focus && "active")}
        onClick={toggleUserbar}
      >
        <div className={classes.selectors}>
          {map(
             selectedJobs,
             jobId => (
               <div
                 key={"job" + jobId}
                 className={classes.jobBullet2}
                 onClick={e => (e.stopPropagation(), deselectJob(jobId))}
               >
                 {allJobs[jobId].name}
               </div>
             )
           )}
        </div>
        <Collapse isOpened={open} springConfig={presets.noWobble}>
          {children}
        </Collapse>
      </div>
    );
  }
}

const styles = {
  bar: {
    position: "absolute",
    backgroundColor: "#FFF",
    color: "#BECBD6",
    lineHeight: "3em",
    boxShadow: "0px 1px 5px 0px #BECBD6",
    padding: "0.5em 0%",
    width: "100%",
    transition: "all 0.3s",
    "&:hover, &.active": {
      boxShadow: "0px 1px 5px 0px #36ABD6"
    }
  },
  selectors: {
    height: "3em",
    "&:first-child": {
      marginLeft: "0.5em"
    }
  },
  jobBullet2: {
    cursor: "pointer",
    color: "#FFF",
    lineHeight: "1em",
    float: "left",
    fontSize: "0.9em",
    fontWeight: "bold",
    backgroundColor: "#7D8B99",
    padding: "0.5em",
    marginLeft: "0.5em",
    marginTop: "0.55em",
    borderRadius: "0.2em"
    
  }
};

const mapStateToProps = ({ userbarOpen }) => ({
  open: userbarOpen
});
const mapDispatchToProps = dispatch => ({
  toggleUserbar: () => Actions.toggleUserbar(dispatch),
  deselectJob: Actions.deselectJob(dispatch)
});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(UserBar)
);
