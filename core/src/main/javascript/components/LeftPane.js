// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

type Props = {
  classes: any,
  className: any,
  children: any
};

const LeftPane = ({ classes, className, children }: Props) => (
  <div className={classNames(className, classes.pane)}>
    {children}
  </div>
);

const styles = {
  pane: {
    float: "left",
    width: "25%",
    backgroundColor: "#2F3647",
    color: "#758390",
    fontFamily: "Arial",
    height: "100vh"
  }
};
export default injectSheet(styles)(LeftPane);
