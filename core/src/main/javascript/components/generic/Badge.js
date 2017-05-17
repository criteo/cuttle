// @flow

import { connect } from "react-redux";
import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

type Props = {
  kind: BadgeKind,
  label: string,
  classes: any,
  className: any
};

export const BadgeKindToken = {
  success: "success",
  fail: "fail",
  info: "info",
  initialized: "initialized",
  header: "header"
};

export type BadgeKind = "success" | "fail" | "info" | "initialized" | "header";

export type BadgeType = { kind: BadgeKind, label: string };

const colors = (kind: BadgeKind): string => {
  switch (kind) {
    case BadgeKindToken.success:
      return "#66CB63";
    case BadgeKindToken.fail:
      return "#FA1E46";
    case BadgeKindToken.info:
      return "#36ABD6";
    case BadgeKindToken.header:
      return "#EFA252";
    case BadgeKindToken.initialized:
    default:
      return "#A6B5C1";

  }
};

const BadgeComponent = ({ classes, className, kind, label }: Props) => {
  return (
    <span
      className={classNames(classes.main, className)}
      style={{ backgroundColor: colors(kind) }}
    >
      {label}
    </span>
  );
};

const styles = {
  main: {
    display: "flex",
    alignItems: "center",
    backgroundColor: "#2B3142",
    color: "#FFF",
    fontFamily: "Arial",
    fontWeight: "bold",
    lineHeight: "1.15em",
    fontSize: "0.75em",
    borderRadius: "2px",
    padding: "0 0.5em",
    paddingBottom: "1px",
    position: "relative",
    top: "-1px"
  }
};
export const Badge = injectSheet(styles)(BadgeComponent);
