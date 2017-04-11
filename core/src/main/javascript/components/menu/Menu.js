// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

import type { PageId } from "../../state";
import MenuEntry from "./MenuEntry";

type Props = {
  activeTab: PageId,
  classes: any,
  className: any
};

const Menu = ({ classes, className, activeTab }: Props) => (
  <nav className={classNames(classes.main, className)}>
    <MenuEntry
      active={activeTab === "monitoring"}
      label="Monitoring"
      link="/monitoring"
      iconName="dashboard"
    />
    <MenuEntry
      active={activeTab === "workflow"}
      label="Workflow"
      link="/workflow"
      iconName="send"
    />
  </nav>
);

const styles = {
  main: {}
};
export default injectSheet(styles)(Menu);
