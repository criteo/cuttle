// @flow

import { connect } from "react-redux";
import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

export type BadgeKind = "success" | "info" | "error" | "warning";

type Props = {
  kind: BadgeKind,
  label: string,
  classes: any,
  className: any,
  width?: number
};

export type BadgeType = { kind: BadgeKind, label: string };

const colors = (kind: BadgeKind): string => {
  switch (kind) {
    case "success":
      return "#66CB63";
    case "error":
      return "#E91E63";
    case "info":
      return "#00BCD4";
    case "warning":
      return "#ff9800";
    default:
      return "#4c515f";

  }
};

const BadgeComponent = ({ classes, className, kind, label, width }: Props) => {
  return (
    <span
      className={classNames(classes.main, className)}
      style={{ backgroundColor: colors(kind), width: width || "auto" }}
    >
      {label}
    </span>
  );
};

const styles = {
  main: {
    display: "inline-block",
    backgroundColor: "#2B3142",
    color: "#FFF",
    fontFamily: "Arial",
    fontWeight: "normal",
    textTransform: "uppercase",
    lineHeight: "18px",
    fontSize: "11px",
    borderRadius: "2px",
    padding: "0 .5em",
    maxHeight: "18px",
    textAlign: "center",
    cursor: "default"
  }
};
export const Badge = injectSheet(styles)(BadgeComponent);
