// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

import type { JobsPage } from "../../ApplicationState";
import MenuEntry from "./MenuEntry";
import MenuSubEntry from "./MenuSubEntry";
import LogIcon from "react-icons/lib/md/playlist-play";
import JobsIcon from "react-icons/lib/go/git-commit";
import CalendarIcon from "react-icons/lib/md/date-range";
import ListIcon from "react-icons/lib/md/format-list-bulleted";
import type { Statistics } from "../../datamodel";

type Props = {
  active: JobsPage,
  statistics: Statistics,
  isTimeseries: boolean,
  classes: any,
  className: any
};

const Menu = ({ classes, className, isTimeseries,active, statistics }: Props) => (
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
              statistics.running && {
                label: statistics.running,
                kind: "info"
              },
              statistics.waiting && {
                label: statistics.waiting,
                kind: "warning"
              },
              statistics.paused && {
                label: statistics.paused
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
      active={active.id === "jobs"}
      label="Jobs"
      link="/jobs"
      icon={
        <JobsIcon
          style={{ transform: "scale(.9) translateX(-1px)" }}
        />
      }
    />
    { isTimeseries ?
        <MenuEntry
          active={active.id.indexOf("timeseries/") === 0 || active.id == "workflow"}
          label="Time series"
          link="/timeseries/calendar"
          icon={<CalendarIcon style={{ transform: "translateY(-3px)" }} />}
          badges={
            active.id.indexOf("timeseries/") === 0
              ? []
              : [
                statistics.scheduler &&
                statistics.scheduler.backfills && {
                  label: statistics.scheduler.backfills,
                  kind: "alt"
                }
              ]
          }
          subEntries={[
            <MenuSubEntry
              active={active.id.indexOf("timeseries/calendar") === 0}
              label="Calendar"
              link="/timeseries/calendar"
            />,
            <MenuSubEntry
              active={active.id === "workflow"}
              label="Workflow"
              link="/workflow"
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
        /> : null
      }
  </nav>
);

const styles = {
  main: {}
};
export default injectSheet(styles)(Menu);
