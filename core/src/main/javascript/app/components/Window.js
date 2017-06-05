// @flow

import React from "react";
import injectSheet from "react-jss";

type Props = {
  classes: any,
  title: string,
  children: any
};

const Window = ({ classes, title, children }: Props) => (
  <div className={classes.container}>
    <div className={classes.overlay}>
      <h1 className={classes.title}>{title}</h1>
      {children}
    </div>
  </div>
);

const styles = {
  container: {
    padding: "1em",
    flex: "1",
    display: "flex",
    flexDirection: "column",
    position: "relative"
  },
  overlay: {
    background: "#ffffff",
    flex: "1",
    position: "relative",
    padding: "1em",
    boxShadow: "0px 0px 15px 0px #799cb7",
    borderRadius: "2px",
    overflow: "hidden",
    display: "flex",
    flexDirection: "column"
  },
  title: {
    fontSize: "1em",
    margin: "-1em -1em 1em -1em",
    padding: "1em",
    color: "#f9fbfc",
    background: "#5c6477",
    fontWeight: "normal",
    boxShadow: "0px 0px 15px 0px #799cb7"
  }
};

export default injectSheet(styles)(Window);
