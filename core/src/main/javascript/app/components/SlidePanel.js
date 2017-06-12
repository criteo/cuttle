// @flow

import React from "react";
import injectSheet from "react-jss";
import classNames from "classnames";

type Props = {
  classes: any,
  title: string,
  children: any
};

type State = {
  open: boolean
};

class SlidePanel extends React.Component {
  state: State;
  props: Props;

  constructor(props: Props) {
    super(props);
    this.state = {
      open: false
    };
  }

  closePanel() {
    this.setState({
      open: false
    });
  }

  openPanel() {
    this.setState({
      open: true
    });
  }

  render() {
    const { classes, children } = this.props;
    const { open } = this.state;
    return (
      <div className={classes.container}>
        <div
          className={classNames(classes.innerContainer, open && "open")}
          onBlur={this.closePanel.bind(this)}
          tabIndex="0"
          onClick={!open && this.openPanel.bind(this)}
        >
          <div className="content">
            {children}
          </div>
        </div>
      </div>
    );
  }
}

const styles = {
  container: {
    position: "absolute",
    right: 0,
    top: 0,
    height: "100%"
  },
  innerContainer: {
    backgroundColor: "rgba(255,255,255,0.75)",
    width: "50px",
    transition: "all .1s ease-out",
    height: "100%",
    border: "none",
    outline: "none",
    overflow: "hidden",
    right: 0,
    boxShadow: "0px 1px 5px 0px #BECBD6",
    "&:hover": {
      backgroundColor: "rgba(255,255,255,0.75)",
      boxShadow: "0px 1px 5px 0px #BECBD6",
      width: "60px"
    },
    "&.open": {
      transition: "none",
      backgroundColor: "rgba(255,255,255,0.95)",
      width: "800px",
      overflowX: "hidden",
      overflowY: "auto"
    },
    "& .content": {
      width: "800px"
    }
  }
};

export default injectSheet(styles)(SlidePanel);
