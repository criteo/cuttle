// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
import { navigate } from "redux-url";
import { connect } from "react-redux";

import type { BadgeType } from "../components/Badge";
import { Badge } from "../components/Badge";
import Link from "../components/Link";
import _ from "lodash";

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

const MenuEntry = ({
  classes,
  className,
  activeClassName,
  icon,
  label,
  link,
  subEntries,
  badges = [],
  active,
  navTo
}: Props) => (
  <div className={classes.menuentry}>
    <Link
      className={classNames(
        classes.main,
        className,
        active && classes.active,
        active && activeClassName
      )}
      href={link}
    >
      <span className={classes.icon}>{icon}</span>
      <span className={classes.label}>{label}</span>
      <div className={classes.badges}>
        {badges
          .filter(x => x)
          .map((b: BadgeType, i) => (
            <Badge
              key={i}
              label={b.label}
              kind={b.kind}
              className={classes.badge}
            />
          ))}
      </div>
    </Link>
    {active ? (
      <div className={classes.content}>
        {subEntries &&
          subEntries.map((e, key) => React.cloneElement(e, { key }))}
      </div>
    ) : null}
  </div>
);

const styles = {
  main: {
    display: "flex",
    width: "90%",
    lineHeight: "1.5em",
    fontFamily: "Arial",
    alignItems: "center",
    fontSize: ".95em",
    color: "#7D8B99",
    padding: "5%",
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
  badges: {
    textAlign: "right",
    margin: "auto 0em auto auto",
    transform: "translateY(-2px)"
  },
  badge: {
    marginLeft: "5px"
  }
};
export default connect(
  () => ({}),
  dispatch => ({
    navTo: link => dispatch(navigate(link))
  })
)(injectSheet(styles)(MenuEntry));
