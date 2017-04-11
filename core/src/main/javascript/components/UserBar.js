// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
import Select from "./generic/Select";

type Props = {
  classes: any,
  className: any,
  children: any
};

class UserBar extends React.Component {
  props: Props;
  state: { focus: boolean };

  constructor(props: Props) {
    super(props);
    this.state = {
      focus: false
    };
  }

  focus() {
    this.setState({ focus: true });
  }

  blur() {
    this.setState({ focus: false });
  }
  render() {
    const { focus } = this.state;
    const { classes, className, children }: Props = this.props;
    return (
      <div className={classNames(className, classes.bar, focus && "active")}>
        <Select
          searchable
          className={classes.searchBox}
          placeholder="Focus on a sub graph by selecting relevant jobs"
          onFocus={() => this.focus()}
          onBlur={() => this.blur()}
        />
      </div>
    );
  }
}

const styles = {
  bar: {
    display: "flex",
    backgroundColor: "#FFF",
    color: "#BECBD6",
    height: "3em",
    lineHeight: "3em",
    boxShadow: "0px 1px 5px 0px #BECBD6",
    padding: "0.5em 0%",
    width: "100%",
    transition: "all 0.3s",
    "&:hover, &.active": {
      boxShadow: "0px 1px 5px 0px #36ABD6"
    }
  },
  searchBox: {
    flex: 1,
    marginLeft: "1em",
    color: "#BECBD6",
    outline: "none",
    "&:focus, &:active": {
      border: "0px",
      outline: "none"
    },
    "& .Select-placeholder": {
      lineHeight: "3em"
    }
  }
};
export default injectSheet(styles)(UserBar);
