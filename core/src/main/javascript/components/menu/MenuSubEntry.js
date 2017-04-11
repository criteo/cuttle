// @flow

import injectSheet from "react-jss";
import React from "react";
import MenuEntry from "./MenuEntry";

type Props = {
  badges: string[],
  label: string,
  onClick: () => void,
  classes: any,
  className: any
};

const MenuSubEntry = ({ classes, label, badges, onClick }: Props) => (
  <MenuEntry
    label={label}
    badges={badges}
    content={null}
    iconName="keyboard_arrow_right"
    className={classes.main}
    activeClassName={classes.active}
    onClick={onClick}
  />
);

const styles = {
  main: {
    height: "1.5em",
    lineHeight: "1.5em",
    backgroundColor: "#2A2F3D"
  },
  active: {
    backgroundColor: "#363D4F"
  }
};

export default injectSheet(styles)(MenuSubEntry);
