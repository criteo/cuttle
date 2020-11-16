// @flow

import React from "react";
import moment from "moment";

import CalendarIcon from "react-icons/lib/md/date-range";
import type { CronContext } from "../datamodel";

type Props = {
  className?: string,
  context: CronContext
};

const Context = ({ context }: Props) => {
  let format = date =>
    moment(date)
      .utc()
      .format("MMM-DD HH:mm");

  return (
    <span>
      <CalendarIcon
        style={{
          fontSize: "1.2em",
          verticalAlign: "middle",
          transform: "translateY(-2px)"
        }}
      />{" "}
      {format(context.instant)} UTC
    </span>
  );
};

export default Context;
