// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
import type { BadgeType } from "../../components/generic/Badge";
import { Badge } from "../../components/generic/Badge";
import Icon from "../../components/generic/Icon";
import map from "lodash/map";

type Props = {
  active: boolean,
  link: string,
  iconName: string,
  label: string,
  subEntries: any[],
  badges: BadgeType[],
  classes: any,
  className: any,
  activeClassName: any
};

const MenuEntry = (
  {
    classes,
    className,
    activeClassName,
    iconName,
    label,
    link,
    subEntries,
    badges,
    active
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
      href={link}
    >
      <Icon iconName={iconName} className={classes.icon} />
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
    height: "2.5em",
    lineHeight: "2.5em",
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
    marginLeft: "0.5em",
    marginRight: "1em"
  },
  label: {},
  badge: {
    margin: "auto 0em auto auto",
    fontVariant: "small-caps"
  }
};
export default injectSheet(styles)(MenuEntry);
