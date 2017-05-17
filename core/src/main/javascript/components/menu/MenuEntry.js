// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
import { navigate } from "redux-url";
import { connect } from "react-redux";

import type { BadgeType } from "../../components/generic/Badge";
import { Badge } from "../../components/generic/Badge";
import map from "lodash/map";

type Props = {
  active: boolean,
  link: string,
  icon: any,
  label: string,
  subEntries: any[],
  badges: BadgeType[],
  classes: any,
  className: any,
  activeClassName: any,
  navTo: any
};

const MenuEntry = (
  {
    classes,
    className,
    activeClassName,
    icon,
    label,
    link,
    subEntries,
    badges,
    active,
    navTo
  }: Props
) => (
  <div className={classes.menuentry}>
    <a
      className={classNames(
        classes.main,
        className,
        active && classes.active,
        active && activeClassName
      )}
      onClick={() => navTo(link)}
    >
      <span className={classes.icon}>{icon}</span>
      <span className={classes.label}>{label}</span>
      {map(badges, (b: BadgeType) => (
        <Badge label={b.label} kind={b.kind} className={classes.badge} />
      ))}

    </a>
    <div className={classes.content}>
      {subEntries}
    </div>
  </div>
);

const styles = {
  main: {
    display: "flex",
    width: "90%",
    lineHeight: "1.5em",
    fontFamily: "Arial",
    alignItems: "center",
    fontSize: "1em",
    color: "#7D8B99",
    padding: "5%",
    textDecoration: "none",
    "&:hover": {
      backgroundColor: "#3B4254",
      color: "#FFF",
      cursor: "pointer"
    }
  },
  active: {
    backgroundColor: "#3B4254",
    color: "#FFF",
    cursor: "pointer"
  },
  icon: {
    marginRight: ".5em",
    fontSize: "1.4em"
  },
  label: {},
  badge: {
    margin: "auto 0em auto auto",
    fontVariant: "small-caps"
  }
};
export default connect(
  () => ({}),
  dispatch => ({
    navTo: link => dispatch(navigate(link))
  })
)(injectSheet(styles)(MenuEntry));
