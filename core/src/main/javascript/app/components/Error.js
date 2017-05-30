// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

import ErrorIcon from "react-icons/lib/md/error";

type Props = {
  classes: any,
  className: any,
  message?: string
};

const items = 3;

const Error = ({ classes, className, message }: Props) => {
  return (
    <div className={classNames(classes.error, className)}>
      <ErrorIcon className={classes.icon} />
      <span>{message}</span>
    </div>
  );
};

const styles = {
  error: {
    display: "flex",
    margin: "auto",
    textAlign: "center",
    paddingBottom: "15%",
    color: "rgb(233, 30, 99)",
    fontSize: ".85em",
    lineHeight: "22px"
  },
  icon: {
    fontSize: "22px",
    marginRight: "5px"
  }
};

export default injectSheet(styles)(Error);
