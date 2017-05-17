// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

import type { PageId } from "../../state";
import MenuEntry from "./MenuEntry";
import MenuSubEntry from "./MenuSubEntry";
import LogIcon from "react-icons/lib/md/playlist-play";
import WorkflowIcon from "react-icons/lib/md/device-hub";

type Props = {
  activeTab: PageId,
  classes: any,
  className: any
};

const Menu = ({ classes, className, activeTab }: Props) => (
  <nav className={classNames(classes.main, className)}>
    <MenuEntry
      active={activeTab.indexOf("executions/") === 0}
      label="Execution log"
      link="/executions/running"
      icon={<LogIcon />}
      subEntries={[
        <MenuSubEntry
          active={activeTab === "executions/running"}
          label="Running"
          link="/executions/running"
        />,
        <MenuSubEntry
          active={activeTab === "executions/stuck"}
          label="Stuck"
          link="/executions/stuck"
        />,
        <MenuSubEntry
          active={activeTab === "executions/finished"}
          label="Finished"
          link="/executions/finished"
        />,
        <MenuSubEntry
          active={activeTab === "executions/paused"}
          label="Paused"
          link="/executions/paused"
        />
      ]}
    />
    <MenuEntry
      active={activeTab === "workflow"}
      label="Workflow"
      link="/workflow"
      icon={
        <WorkflowIcon
          style={{ transform: "rotate(90deg) scale(.9) translateX(-1px)" }}
        />
      }
    />
  </nav>
);

const styles = {
  main: {}
};
export default injectSheet(styles)(Menu);
