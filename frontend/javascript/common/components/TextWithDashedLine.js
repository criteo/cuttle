// @flow

import React from "react";
import injectSheet from "react-jss";

const TextWithDashedLine = ({
  text,
  classes
}: {
  text: string,
  classes: { waiting: Object }
}) => (
  <div className={classes.waiting}>
    <svg
      width="100%"
      height="2"
      version="1.1"
      xmlns="http://www.w3.org/2000/svg"
    >
      <line x1="-10" y1="0" x2="100%" y2="0" />
    </svg>
    <span>{text}</span>
  </div>
);

const styles = {
  waiting: {
    position: "relative",
    margin: "5px 0",
    "& line": {
      stroke: "#FFFF91",
      strokeWidth: "2px",
      strokeDasharray: "5,5",
      animation: "dashed 500ms linear infinite"
    },
    "& span": {
      color: "#FFFF91",
      position: "absolute",
      textAlign: "center",
      left: "50%",
      top: "2px",
      display: "inline-block",
      width: "180px",
      marginLeft: "-90px",
      background: "#23252f"
    }
  }
};

export default injectSheet(styles)(TextWithDashedLine);
