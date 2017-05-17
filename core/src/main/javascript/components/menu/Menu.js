// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

import type { PageId } from "../../state";
import MenuEntry from "./MenuEntry";
import WorkflowIcon from "react-icons/lib/md/device-hub";

type Props = {
  activeTab: PageId,
  classes: any,
  className: any
};

const Menu = ({ classes, className, activeTab }: Props) => (
  <nav className={classNames(classes.main, className)}>
    <MenuEntry
      active={activeTab === "workflow"}
      label="Workflow"
      link="/workflow"
      icon={<WorkflowIcon style={{ transform: "rotate(90deg)" }} />}
    />
  </nav>
);

const styles = {
  main: {}
};
export default injectSheet(styles)(Menu);
