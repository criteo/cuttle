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
    position: "fixed",
    top: "0",
    left: "0",
    right: "0",
    bottom: "0",
    padding: "2em",
    display: "flex",
    flexDirection: "column",
    zIndex: "99999",
    background: "rgba(53, 57, 68, 0.95)"
  },
  overlay: {
    background: "#ffffff",
    flex: "1",
    position: "relative",
    padding: "1em",
    boxShadow: "0px 0px 15px 0px #12202b",
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
