// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
import noop from "lodash/noop";

import LabelIcon from "react-icons/lib/md/label";

type Props = {
  classes: any,
  className: any,
  name: string,
  color: string,
  active: boolean,
  verbose: boolean,
  onClick: () => void
};

const TagBullet = ({
  classes,
  className,
  name,
  color,
  active,
  verbose,
  onClick
}: Props) => {
  return (
    <div
      className={classNames(
        classes.main,
        className,
        active && "active",
        onClick && "clickable"
      )}
      onClick={onClick ? e => (e.stopPropagation(), onClick()) : noop}
      title={name}
    >
      <LabelIcon style={{ color: color }} />
      {verbose && <span className="tagName">{name}</span>}
    </div>
  );
};

const styles = {
  main: {
    color: "#2F3647",
    padding: "0.3em",
    paddingRight: "0.5em",
    fontSize: "0.9em",
    lineHeight: "1.5em",
    borderRadius: "0.25em",
    "& i": {
      fontSize: "20px",
      marginRight: "0.15em",
      verticalAlign: "middle"
    },
    "&.clickable": {
      cursor: "pointer"
    },
    "&.active, &.clickable:hover": {
      backgroundColor: "#2F3647",
      color: "#FFF"
    }
  }
};
export default injectSheet(styles)(TagBullet);
