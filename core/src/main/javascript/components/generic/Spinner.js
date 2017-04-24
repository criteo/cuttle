// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
import times from "lodash/times";

type Props = {
  dark: boolean,
  classes: any,
  className: any
};

const SpinnerComponent = ({ classes, className, dark }: Props) => {
  return (
    <div className={classNames(classes.spinner, className)}>
      {times(5, i => (
        <div
          key={i}
          className={classNames(
            classes.rect,
            classes["rect" + i],
            dark ? classes.darkRect : classes.lightRect
          )}
        />
      ))}
    </div>
  );
};

const styles = {
  spinner: {
    margin: "100px auto",
    width: "50px",
    height: "40px",
    textAlign: "center",
    fontSize: "10px"
  },
  rect: {
    height: "100%",
    width: "6px",
    margin: "0px 1px",
    display: "inline-block",
    animation: "animation 1.2s infinite ease-in-out"
  },
  darkRect: {
    backgroundColor: "#333"
  },
  lightRect: {
    backgroundColor: "#EEE"
  },
  "@keyframes animation": {
    "0%, 40%, 100%": {
      transform: "scaleY(0.4)"
    },
    "20%": {
      transform: "scaleY(1.0)"
    }
  }
};

times(5, i => {
  styles["rect" + i] = {
    animationDelay: "-" + (1.3 - i * 0.1) + "s"
  };
});

export default injectSheet(styles)(SpinnerComponent);
