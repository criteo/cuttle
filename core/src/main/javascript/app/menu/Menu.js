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
import ListIcon from "react-icons/lib/md/format-list-bulleted";
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
              statistics.waiting && {
                label: statistics.waiting,
                kind: "warning"
              },
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
          active={active.id.indexOf("timeseries/calendar") === 0}
          label="Calendar"
          link="/timeseries/calendar"
        />,
        <MenuSubEntry
          active={active.id === "timeseries/backfills"}
          label="Backfills"
          link="/timeseries/backfills"
          badges={[
            statistics.scheduler &&
            statistics.scheduler.backfills && {
              label: statistics.scheduler.backfills,
              kind: "alt"
            }
          ]}
        />
      ]}
    />
    <MenuEntry
      active={active.id.indexOf("jobs/") === 0}
      label="Jobs"
      link="/jobs/all"
      icon={<ListIcon style={{ transform: "translateY(-3px)" }} />}
      subEntries={[
        <MenuSubEntry
          active={active.id.indexOf("jobs/all") === 0}
          label="All"
          link="/jobs/all"
        />,
        <MenuSubEntry
          active={active.id.indexOf("jobs/active") === 0}
          label="Active"
          link="/jobs/active"
        />,
        <MenuSubEntry
          active={active.id.indexOf("jobs/paused") === 0}
          label="Paused"
          link="/jobs/paused"
        />
      ]}
    />
  </nav>
);

const styles = {
  main: {}
};
export default injectSheet(styles)(Menu);
