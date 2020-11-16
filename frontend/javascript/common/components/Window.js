// @flow

import React from "react";
import injectSheet from "react-jss";
import { connect } from "react-redux";
import { goBack, navigate } from "redux-url";
import CloseIcon from "react-icons/lib/md/close";

type Props = {
  classes: any,
  title: string,
  children: any,
  back: () => void,
  closeUrl: ?string
};

const Window = ({ classes, title, children, back }: Props) => (
  <div className={classes.container}>
    <div className={classes.overlay}>
      <h1 className={classes.title}>{title}</h1>
      <CloseIcon className={classes.close} onClick={back} />
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
    padding: "2em 5em",
    display: "flex",
    flexDirection: "column",
    zIndex: "99999",
    background: "rgba(53, 57, 68, 0.95)"
  },
  overlay: {
    background: "#ffffff",
    flex: "1",
    position: "relative",
    boxShadow: "0px 0px 15px 0px #12202b",
    borderRadius: "2px",
    overflow: "hidden",
    display: "flex",
    flexDirection: "column"
  },
  title: {
    fontSize: "1em",
    margin: "0",
    padding: "1em",
    color: "#f9fbfc",
    background: "#5f6b86",
    fontWeight: "normal",
    boxShadow: "0px 0px 15px 0px #799cb7"
  },
  close: {
    position: "absolute",
    color: "#eef5fb",
    top: ".75em",
    right: ".5em",
    cursor: "pointer",
    fontSize: "20px"
  }
};

const mapDispatchToProps = (dispatch, ownProps) => ({
  back() {
    if (ownProps.closeUrl) {
      dispatch(navigate(ownProps.closeUrl));
    } else {
      dispatch(goBack());
    }
  }
});

export default connect(
  null,
  mapDispatchToProps
)(injectSheet(styles)(Window));
