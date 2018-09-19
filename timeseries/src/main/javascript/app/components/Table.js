// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
import AscIcon from "react-icons/lib/md/keyboard-arrow-down";
import DescIcon from "react-icons/lib/md/keyboard-arrow-up";

type Column = {
  id: string,
  label?: string,
  width?: number,
  sortable?: boolean
};

type Props = {
  classes: any,
  className: any,
  envCritical: boolean,
  columns: Array<Column>,
  onSortBy: (column: string) => void,
  sort: { column: string, order: "asc" | "desc" },
  data: Array<any>,
  render: (column: string, row: any) => any
};

export const ROW_HEIGHT = 43;

const Table = ({
  classes,
  className,
  envCritical,
  sort,
  onSortBy,
  columns,
  data,
  render
}: Props) => {
  return (
    <table
      className={classNames(classes.table, className, {
        [classes.critical]: envCritical
      })}
    >
      <thead>
        <tr>
          {columns.map(({ id, label, width, sortable }) => {
            let isSorted = sort.column == id ? sort.order : null;
            return (
              <th
                key={id}
                width={width || "auto"}
                onClick={() => onSortBy(id)}
                className={classNames({ [classes.sortable]: sortable })}
              >
                <div>
                  {label}
                  {isSorted == "asc"
                    ? <AscIcon className={classes.sortIcon} />
                    : null}
                  {isSorted == "desc"
                    ? <DescIcon className={classes.sortIcon} />
                    : null}
                  {!isSorted
                    ? <AscIcon
                        className={classes.sortIcon}
                        style={{ color: "transparent" }}
                      />
                    : null}
                </div>
              </th>
            );
          })}
        </tr>
      </thead>
      <tbody>
        {data.map((row, i) => {
          return (
            <tr key={i}>
              {columns.map(({ id }) => {
                return (
                  <td key={id}>
                    <div>{render(id, row)}</div>
                  </td>
                );
              })}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
};

const styles = {
  table: {
    borderSpacing: "0",
    fontSize: ".9em",
    width: "100%",
    background: "#ffffff",
    "& thead": {
      color: "#303a41",
      boxShadow: "0px 1px 2px #BECBD6"
    },
    "& tr": {
      height: ROW_HEIGHT,
      padding: "0",
      textAlign: "left",
      boxSizing: "border-box",
      transition: "100ms",
      "&:hover td:first-child::after": {
        content: "''",
        position: "absolute",
        left: "0",
        top: "0",
        width: "3px",
        background: "#26a69a",
        bottom: "0"
      }
    },
    "& th": {
      background: "#f5f8fa",
      height: "46px",
      boxSizing: "border-box",
      cursor: "default"
    },
    "& td": {
      borderBottom: "1px solid #ecf1f5",
      position: "relative"
    },
    "& td div, & th div": {
      padding: "0 15px"
    }
  },
  sortable: {
    cursor: "pointer !important",
    userSelect: "none"
  },
  sortIcon: {
    color: "#6f98b1"
  },
  critical: {
    "& tr:hover td:first-child::after": {
      background: "#ff5722 !important"
    }
  }
};

export default injectSheet(styles)(Table);
