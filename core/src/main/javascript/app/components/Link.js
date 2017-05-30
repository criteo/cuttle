// @flow

import React from "react";
import { connect } from "react-redux";
import { navigate } from "redux-url";

type Props = {
  href: string,
  className: string,
  children: any,
  open: (href: string) => void,
  replace: ?boolean
};

const Link = ({ href, className, children, open, replace = false }) => {
  return (
    <a onClick={open(href, replace)} href={href} className={className}>
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

export default connect(mapStateToProps, mapDispatchToProps)(Link);
