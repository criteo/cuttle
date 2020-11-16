// @flow

import React from "react";
import injectSheet from "react-jss";

type Props = {
  classes: any,
  key: string,
  children: any
};

const FancyTableComponent = ({ classes, key, children }: Props) => (
  <dl className={classes.definitions} key={key}>
    {children}
  </dl>
);

const styles = {
  definitions: {
    flex: "none",
    overflow: "auto",
    margin: "0",
    display: "flex",
    flexFlow: "row",
    flexWrap: "wrap",
    fontSize: ".85em",
    background: "rgba(189, 213, 228, 0.1)",
    "& dt": {
      flex: "0 0 150px",
      textOverflow: "ellipsis",
      overflow: "hidden",
      padding: "0 1em",
      boxSizing: "border-box",
      textAlign: "right",
      color: "#637686",
      lineHeight: "2.75em"
    },
    "& dd": {
      flex: "0 0 calc(100% - 150px)",
      marginLeft: "auto",
      textAlign: "left",
      textOverflow: "ellipsis",
      overflow: "hidden",
      padding: "0",
      boxSizing: "border-box",
      lineHeight: "2.75em"
    },
    "& dd:nth-of-type(even), & dt:nth-of-type(even)": {
      background: "#eef5fb"
    }
  }
};

export default injectSheet(styles)(FancyTableComponent);
