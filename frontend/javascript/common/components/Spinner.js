// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
import times from "lodash/times";

type Props = {
  classes: any,
  className: any
};

const items = 3;

const SpinnerComponent = ({ classes, className }: Props) => {
  return (
    <div className={classNames(classes.spinner, className)}>
      {times(items, i => (
        <div
          key={i}
          className={classNames(classes.item, classes["item" + i])}
        />
      ))}
    </div>
  );
};

const styles = {
  spinner: {
    display: "flex",
    margin: "auto",
    textAlign: "center",
    paddingBottom: "15%"
  },
  item: {
    height: "6px",
    width: "6px",
    borderRadius: "50%",
    margin: "0px 2px",
    display: "inline-block",
    animation: "animation 1.2s infinite ease-in-out",
    backgroundColor: "#607e96"
  },
  "@keyframes animation": {
    "0%, 40%, 100%": {
      transform: "scale(0.5)"
    },
    "20%": {
      transform: "scale(1.1)"
    }
  }
};

times(items, i => {
  styles["item" + i] = {
    animationDelay: "-" + (1.3 - i * 0.1) + "s"
  };
});

export default injectSheet(styles)(SpinnerComponent);
