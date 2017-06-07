// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

import type { Page } from "../../ApplicationState";
import MenuEntry from "./MenuEntry";
import MenuSubEntry from "./MenuSubEntry";
import LogIcon from "react-icons/lib/md/playlist-play";
import WorkflowIcon from "react-icons/lib/go/git-merge";
import CalendarIcon from "react-icons/lib/md/date-range";
import type { Statistics } from "../../datamodel";

type Props = {
  active: Page,
  statistics: Statistics,
  classes: any,
  className: any
};

const Menu = ({ classes, className, active, statistics }: Props) => (
  <nav className={classNames(classes.main, className)}>
    <MenuEntry
      active={active.id.indexOf("executions/") === 0}
      label="Executions"
      link="/executions/started"
      icon={<LogIcon />}
      badges={
        active.id.indexOf("executions/") === 0
          ? []
          : [
              statistics.running && { label: statistics.running, kind: "info" },
              statistics.waiting && { label: statistics.waiting, kind: "warning" },
              statistics.failing && {
                label: statistics.failing,
                kind: "error"
              }
            ]
      }
      subEntries={[
        <MenuSubEntry
          active={active.id === "executions/started"}
          label="Started"
          link="/executions/started"
          badges={[
            statistics.running && { label: statistics.running, kind: "info" },
            statistics.waiting && { label: statistics.waiting, kind: "warning" }
          ]}
        />,
        <MenuSubEntry
          active={active.id === "executions/stuck"}
          label="Stuck"
          link="/executions/stuck"
          badges={[
            statistics.failing && { label: statistics.failing, kind: "error" }
          ]}
        />,
        <MenuSubEntry
          active={active.id === "executions/finished"}
          label="Finished"
          link="/executions/finished"
        />,
        <MenuSubEntry
          active={active.id === "executions/paused"}
          label="Paused"
          link="/executions/paused"
          badges={[statistics.paused && { label: statistics.paused }]}
        />
      ]}
    />
    <MenuEntry
      active={active.id === "workflow"}
      label="Workflow"
      link="/workflow"
      icon={
        <WorkflowIcon
          style={{ transform: "rotate(90deg) scale(.9) translateX(-1px)" }}
        />
      }
    />
    <MenuEntry
      active={active.id.indexOf("timeseries/") === 0}
      label="Time series"
      link="/timeseries/calendar"
      icon={<CalendarIcon style={{ transform: "translateY(-3px)" }} />}
      subEntries={[
        <MenuSubEntry
          active={active.id === "timeseries/calendar"}
          label="Calendar"
          link="/timeseries/calendar"
        />,
        <MenuSubEntry
          active={active.id === "timeseries/backfills"}
          label="Backfills"
          link="/timeseries/backfills"
        />
      ]}
    />
  </nav>
);

const styles = {
  main: {}
};
export default injectSheet(styles)(Menu);
