// @flow

import React from "react";
import classNames from "classnames";
import injectSheet from "react-jss";
import { connect } from "react-redux";
import { navigate } from "redux-url";

type Props = {
  classes: any,
  href: string,
  className: string,
  children: any,
  open: (href: string) => void,
  replace: ?boolean
};

const Link = ({
  classes,
  href,
  className,
  children,
  open,
  replace = false
}) => {
  return (
    <a
      onClick={open(href, replace)}
      href={href}
      className={classNames(classes.link, className)}
    >
      {children}
    </a>
  );
};

const mapStateToProps = ({}) => ({});
const mapDispatchToProps = dispatch => ({
  open(href, replace) {
    return e => {
      e.preventDefault();
      dispatch(navigate(href, replace));
    };
  }
});

const styles = {
  link: {
    textDecoration: "none",
    color: "inherit"
  }
};

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(Link)
);
