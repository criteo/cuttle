// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
import { Badge } from "../components/Badge";
import Link from "../components/Link";

type Props = {
  projectName: string,
  projectVersion: ?string,
  env: {
    name: ?string,
    critical: boolean
  },
  classes: any,
  className: any
};

const MenuHeader = ({
  classes,
  className,
  env,
  projectName,
  projectVersion
}: Props) => (
  <Link className={classNames(classes.main, className)} href="/">
    <span title={projectName} className={classes.projectName}>{projectName}</span>
    {projectVersion && (
      <span className={classes.projectVersion}>{projectVersion}</span>
    )}
    {env.name ? (
      <Badge
        label={env.name}
        className={classNames(classes.badge, {
          [classes.badgeCritical]: env.critical
        })}
      />
    ) : null}
  </Link>
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
    padding: "0.5em 5%",
    cursor: "pointer"
  },
  icon: {
    marginRight: ".5em",
    fontSize: "1.4em",
    color: "#fc1246"
  },
  projectName: {
    fontWeight: "bold",
    fontSize: "1.2em",
    whiteSpace: "nowrap",
    textOverflow: "ellipsis",
    overflow: "hidden"
  },
  projectVersion: {
    fontSize: ".65em",
    paddingLeft: ".5em",
    position: "relative",
    top: "3px",
    opacity: ".6"
  },
  badge: {
    margin: "auto 0em auto auto",
    backgroundColor: "#26a69a !important"
  },
  badgeCritical: {
    backgroundColor: "#FF5722 !important"
  }
};

export default injectSheet(styles)(MenuHeader);
