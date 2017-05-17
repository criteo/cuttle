// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
import Select from "react-select";

type Props = {
  searchable: boolean,
  onFocus: () => void,
  onBlur: () => void,
  placeholder: string,
  classes: any,
  className: any
};

const SelectComponent = (
  { classes, className, searchable, placeholder, onFocus, onBlur }: Props
) => {
  return (
    <Select
      searchable={searchable}
      className={classNames(classes.main, className)}
      placeholder={placeholder}
      onFocus={onFocus}
      onBlur={onBlur}
    />
  );
};

const styles = {
  main: {
    outline: "none",
    border: "0px",
    "& .Select-control": {
      border: "0px",
      "&:hover": {
        boxShadow: "none"
      },
      "& .Select-input": {
        display: "inline !important"
      },
      "& .Select-menu-outer": {
        borderRadius: "0px",
        border: "0px",
        boxShadow: "none"
      },
      "& .Select-arrow-zone": {
        display: "none"
      }
    }
  }
};

export default injectSheet(styles)(SelectComponent);
