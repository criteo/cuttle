// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

import type { DagsPage } from "./ApplicationState";
import MenuEntry from "../common/menu/MenuEntry";
import MenuSubEntry from "../common/menu/MenuSubEntry";
import LogIcon from "react-icons/lib/md/playlist-play";
import ListIcon from "react-icons/lib/md/format-list-bulleted";
import type { Statistics } from "./datamodel";

type Props = {
  active: DagsPage,
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
          active={active.id === "executions/retrying"}
          label="Retrying"
          link="/executions/retrying"
          badges={[
            statistics.failing && { label: statistics.failing, kind: "error" }
          ]}
        />,
        <MenuSubEntry
          active={active.id === "executions/finished"}
          label="Finished"
          link="/executions/finished"
        />
      ]}
    />
    <MenuEntry
      active={active.id === "dags"}
      label="Dags"
      link="/dags/all"
      icon={<ListIcon style={{ transform: "translateY(-3px)" }} />}
      subEntries={[
        <MenuSubEntry
          active={active.id === "dags" && active.status === "all"}
          label="All"
          link="/dags/all"
        />,
        <MenuSubEntry
          active={active.id === "dags" && active.status === "active"}
          label="Active"
          link="/dags/active"
        />,
        <MenuSubEntry
          active={active.id === "dags" && active.status === "paused"}
          label="Paused"
          link="/dags/paused"
        />
      ]}
    />
  </nav>
);

const styles = {
  main: {}
};
export default injectSheet(styles)(Menu);
