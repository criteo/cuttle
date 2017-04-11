// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
import { Badge, BadgeKindToken } from "../../components/generic/Badge";
import Icon from "../../components/generic/Icon";

type Props = {
  icon: string,
  workflowName: string,
  environment: string,
  classes: any,
  className: any
};

const MenuHeader = ({ classes, className, environment, workflowName }: Props) => (
  <div className={classNames(classes.main, className)}>
    <Icon className={classes.icon} iconName="rowing" />
    <span className={classes.workflowName}>{workflowName}</span>
    <Badge
      label={environment}
      kind={BadgeKindToken.header}
      className={classes.badge}
    />
  </div>
);

const styles = {
  main: {
    display: "flex",
    width: "90%",
    height: "3em",
    lineHeight: "3em",
    backgroundColor: "#2B3142",
    color: "#FFF",
    fontFamily: "Arial",
    alignItems: "center",
    padding: "0.5em 5%"
  },
  icon: {
    marginLeft: "0.5em",
    marginRight: "1em"
  },
  workflowName: {
    fontWeight: "bold",
    fontSize: "1.2em"
  },
  badge: {
    margin: "auto 0em auto auto",
    fontSize: "1em",
    fontVariant: "small-caps"
  }
};
export default injectSheet(styles)(MenuHeader);
