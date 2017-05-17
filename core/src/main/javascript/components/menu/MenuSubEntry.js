// @flow

import injectSheet from "react-jss";
import React from "react";
import MenuEntry from "./MenuEntry";

import DotIcon from "react-icons/lib/go/primitive-dot";

type Props = {
  badges: string[],
  active: boolean,
  label: string,
  link: string,
  classes: any,
  className: any
};

const MenuSubEntry = ({ active, classes, label, badges, link }: Props) => (
  <MenuEntry
    active={active}
    label={<span className={classes.label}>{label}</span>}
    badges={badges}
    content={null}
    icon={
      <DotIcon
        className={classes.dot}
        style={{ color: active ? "#2eacd7" : "#ffffff" }}
      />
    }
    className={classes.main}
    activeClassName={classes.active}
    link={link}
  />
);

const styles = {
  main: {
    height: "1em",
    lineHeight: "1em",
    backgroundColor: "#2A2F3D",
    "&:hover": {
      backgroundColor: "#363D4F"
    }
  },
  active: {
    backgroundColor: "#363D4F"
  },
  dot: {
    transform: "scale(.5) translateX(-4px)"
  },
  label: {
    fontSize: "85%"
  }
};

export default injectSheet(styles)(MenuSubEntry);
