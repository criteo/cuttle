// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

import type { PageId } from "../../state";
import MenuEntry from "./MenuEntry";
import MenuSubEntry from "./MenuSubEntry";
import LogIcon from "react-icons/lib/md/playlist-play";
import WorkflowIcon from "react-icons/lib/md/device-hub";
import CalendarIcon from "react-icons/lib/md/date-range";
import type { Statistics } from "../../datamodel";

type Props = {
  activeTab: PageId,
  statistics: Statistics,
  classes: any,
  className: any
};

const Menu = ({ classes, className, activeTab, statistics }: Props) => (
  <nav className={classNames(classes.main, className)}>
    <MenuEntry
      active={activeTab.indexOf("executions/") === 0}
      label="Execution log"
      link="/executions/started"
      icon={<LogIcon />}
      badges={
        activeTab.indexOf("executions/") === 0
          ? []
          : [
              statistics.running && { label: statistics.running, kind: "info" },
              statistics.failing && {
                label: statistics.failing,
                kind: "error"
              },
              statistics.paused && {
                label: statistics.paused,
                kind: "warning"
              }
            ]
      }
      subEntries={[
        <MenuSubEntry
          active={activeTab === "executions/started"}
          label="Started"
          link="/executions/started"
          badges={[
            statistics.running && { label: statistics.running, kind: "info" }
          ]}
        />,
        <MenuSubEntry
          active={activeTab === "executions/stuck"}
          label="Stuck"
          link="/executions/stuck"
          badges={[
            statistics.failing && { label: statistics.failing, kind: "error" }
          ]}
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
          badges={[statistics.paused && { label: statistics.paused, kind: "warning" }]}
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
    <MenuEntry
      active={activeTab.indexOf("timeseries/") === 0}
      label="Time series"
      link="/timeseries/calendar"
      icon={<CalendarIcon style={{ transform: "translateY(-3px)" }} />}
      subEntries={[
        <MenuSubEntry
          active={activeTab === "timeseries/calendar"}
          label="Calendar"
          link="/timeseries/calendar"
        />,
        <MenuSubEntry
          active={activeTab === "timeseries/backfill"}
          label="Backfill"
          link="/timeseries/backfill"
        />
      ]}
    />
  </nav>
);

const styles = {
  main: {}
};
export default injectSheet(styles)(Menu);
