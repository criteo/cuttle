// @flow

import injectSheet from "react-jss";
import React from "react";
import moment from "moment";

import Link from "./Link";
import ContextIcon from "react-icons/lib/md/input";
import { urlFormat } from "../utils/Date";
import TimeRangeLink from "./TimeRangeLink";

type Props = {
  classes: any,
  className?: string,
  context: any
};

const Context = ({ classes, context }: Props) => {
  if(context.start && context.end && ("backfill" in context)) {
    const { start, end } = context;
    return (
      <TimeRangeLink
        href={`/timeseries/calendar/${urlFormat(start)}_${urlFormat(end)}`}
        start={start}
        end={end}
      />
    );
  }
  else {
    let ctxString = JSON.stringify(context);
    return (
      <span className={classes.badge}>
        <ContextIcon
          style={{
            fontSize: "1.2em",
            marginRight: "5px",
            verticalAlign: "top",
            transform: "translateY(-1px)"
          }}
        />
        {ctxString.length < 50 ? ctxString : ctxString.substring(0, 50) + '...'}
      </span>
    );
  }
};

const styles = {
  badge: {
    whiteSpace: 'no-wrap'
  }
};

export default  injectSheet(styles)(Context);
