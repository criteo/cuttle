// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
import { Badge } from "../components/Badge";
import AppIcon from "react-icons/lib/md/fiber-smart-record";

type Props = {
  projectName: string,
  environment: string,
  classes: any,
  className: any
};

const MenuHeader = ({
  classes,
  className,
  environment,
  projectName
}: Props) => (
  <div className={classNames(classes.main, className)}>
    <span className={classes.projectName}>{projectName}</span>
    <Badge label={environment} className={classes.badge} />
  </div>
);

const styles = {
  main: {
    display: "flex",
    minHeight: "3em",
    lineHeight: "3em",
    backgroundColor: "#2B3142",
    color: "#FFF",
    fontFamily: "Arial",
    alignItems: "center",
    padding: "0.5em 5%"
  },
  icon: {
    marginRight: ".5em",
    fontSize: "1.4em",
    color: "#fc1246"
  },
  projectName: {
    fontWeight: "bold",
    fontSize: "1.2em"
  },
  badge: {
    margin: "auto 0em auto auto",
    backgroundColor: "#FF5722 !important"
  }
};

export default injectSheet(styles)(MenuHeader);
