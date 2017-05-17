// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

import SearchIcon from "react-icons/lib/md/search";

type Props = {
  classes: any,
  className: any,
  placeholder: string,
  onChange: () => void,
  onKeyDown: () => void,
  defaultValue: string,
  autoFocus: boolean,
  icon: boolean
};

const SearchBox = (
  {
    classes,
    className,
    placeholder,
    onChange,
    defaultValue,
    autoFocus,
    icon = false,
    onKeyDown
  }: Props
) => (
  <div className={classNames(classes.searchBox, className)}>
    {icon && <SearchIcon />}
    <input
      type="text"
      className="inputText"
      defaultValue={defaultValue}
      onChange={onChange}
      placeholder={placeholder}
      onKeyDown={onKeyDown}
      autoFocus={autoFocus}
    />
  </div>
);

const styles = {
  searchBox: {
    display: "flex",
    "& i": {
      verticalAlign: "middle"
    },
    "& input": {
      marginLeft: "1em",
      verticalAlign: "middle",
      flexGrow: 1,
      borderRadius: 0,
      border: "none",
      outline: "none",
      "&:active, &:focus, &:hover": {
        border: "none",
        backgroundColor: "#FFF",
        color: "#2F3647"
      }
    }
  }
};

export default injectSheet(styles)(SearchBox);
