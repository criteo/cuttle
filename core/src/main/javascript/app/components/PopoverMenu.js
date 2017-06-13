// @flow

import injectSheet from "react-jss";
import { connect } from "react-redux";
import classNames from "classnames";
import React from "react";

import Icon from "react-icons/lib/md/more-vert";

type Props = {
  classes: any,
  className: string,
  envCritical: boolean,
  items: Array<Node>
};

type State = {
  open: boolean
};

class PopoverMenu extends React.Component {
  props: Props;
  state: State;

  constructor(props: Props) {
    super(props);
    this.state = { open: false };
  }

  render() {
    let { classes, className, envCritical, items } = this.props;

    let open = () => {
      this.setState({ open: true });
    };

    let close = () => {
      this.setState({ open: false });
    };

    return (
      <div className={classNames(className, {[classes.critical]: envCritical})}>
        <Icon className={classes.icon} onClick={open} />
        {this.state.open
          ? <div className={classes.overlay} onClick={close} />
          : null}
        {this.state.open
          ? <ul className={classes.menu}>
              {items.map((item, i) => <li key={i} onClick={close}>{item}</li>)}
            </ul>
          : null}
      </div>
    );
  }
}

const styles = {
  icon: {
    fontSize: "1.35em",
    cursor: "pointer"
  },
  overlay: {
    background: "transparent",
    position: "fixed",
    left: "0",
    right: "0",
    top: "0",
    bottom: "0",
    zIndex: 10000
  },
  menu: {
    background: "#ffffff",
    listStyle: "none",
    margin: "0",
    padding: ".25em 0",
    position: "absolute",
    width: "auto",
    right: "-.4em",
    top: "-.2em",
    boxShadow: "0px 5px 20px rgba(0,0,0,.3), 0 0 0 1px #eee",
    zIndex: 10001,

    "& li": {
      whiteSpace: "nowrap",
      fontSize: ".9em",
      cursor: "pointer",
      padding: ".5em 1.5em",

      "&:hover": {
        background: "#26a69a",
        color: "#ffffff"
      },

      "& a": {
        color: "inherit",
        textDecoration: "none"
      }
    }
  },
  critical: {
    "& li:hover": {
      background: "#FF5722"
    }
  }
};

const mapStateToProps = ({ project }) => ({
  envCritical: project.env.critical
});

export default connect(mapStateToProps)(injectSheet(styles)(PopoverMenu));
