// @flow

import React from "react";
import moment from "moment";

import Link from "./Link";
import CalendarIcon from "react-icons/lib/md/date-range";
import BreakIcon from "react-icons/lib/md/keyboard-control";

type Props = {
  className?: string,
  context: {
    start: string,
    end: string
  }
};

const Context = ({ context }: Props) => {
  // Need to be dynamically linked with the scehduler but for now let's
  // assume that it is a TimeseriesContext
  let format = date => moment(date).utc().format("MMM-DD HH:mm");
  let URLFormat = date => moment(date).utc().format("YYYY-MM-DDTHH") + "Z";

  return (
    <Link
      href={`/timeseries/calendar/${URLFormat(context.start)}_${URLFormat(context.end)}`}
    >
      <CalendarIcon
        style={{
          fontSize: "1.2em",
          verticalAlign: "middle",
          transform: "translateY(-2px)"
        }}
      />
      {" "}
      {format(context.start)}
      {" "}
      <BreakIcon />
      {" "}
      {format(context.end)} UTC
    </Link>
  );
};

export default Context;
