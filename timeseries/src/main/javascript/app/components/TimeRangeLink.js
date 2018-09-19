// @flow

import React from "react";

import BreakIcon from "react-icons/lib/md/keyboard-control";
import CalendarIcon from "react-icons/lib/md/date-range";

import Link from "../components/Link";
import { displayFormat } from "../utils/Date";

type Props = {
  href: string,
  start: Date,
  end: Date
};

const TimeRangeLink = ({ href, start, end }: Props) => {
  return (
    <Link href={href}>
      <CalendarIcon
        style={{
          fontSize: "1.2em",
          verticalAlign: "top",
          transform: "translateY(-1px)"
        }}
      />
      {" "}
      {displayFormat(start)}
      {" "}
      <BreakIcon />
      {" "}
      {displayFormat(end)} UTC
    </Link>
  );
};

export default TimeRangeLink;
