// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

type Props = {
  classes: any,
  className: any,
  children: any
};

const RightPane = ({ classes, className, children }: Props) => (
  <div className={classNames(className, classes.pane)}>
    {children}
  </div>
);

const styles = {
  pane: {
    backgroundColor: "#ECF1F5",
    color: "#3D4454",
    fontFamily: "Arial"
  }
};

export default injectSheet(styles)(RightPane);
