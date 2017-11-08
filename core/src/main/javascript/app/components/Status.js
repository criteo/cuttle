// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
import { Badge } from "../components/Badge";

type Props = {
  classes: any,
  className: any,
  status: string,
  labelFormatter?: string => string
};

const StatusComponent = ({
  classes,
  className,
  status,
  labelFormatter = s => s
}: Props) => {
  const badgeClassName = classNames(classes.main, className);

  switch (status) {
    case "running":
      return (
        <Badge
          className={badgeClassName}
          label={labelFormatter("RUNNING")}
          kind="info"
          width={75}
        />
      );
    case "throttled":
      return (
        <Badge
          className={badgeClassName}
          label={labelFormatter("RETRYING")}
          kind="error"
          light={true}
          width={75}
        />
      );
    case "failed":
      return (
        <Badge
          className={badgeClassName}
          label={labelFormatter("FAILED")}
          kind="error"
          width={75}
        />
      );
    case "successful":
      return (
        <Badge
          className={badgeClassName}
          label={labelFormatter("SUCCESS")}
          kind="success"
          width={75}
        />
      );
    case "paused":
      return (
        <Badge
          className={badgeClassName}
          label={labelFormatter("PAUSED")}
          width={75}
          light={true}
        />
      );
    case "waiting":
      return (
        <Badge
          className={badgeClassName}
          label={labelFormatter("WAITING")}
          kind="warning"
          width={75}
        />
      );
    default:
      return (
        <Badge
          className={badgeClassName}
          label={labelFormatter(status)}
          width={75}
        />
      );
  }
};

const styles = {};

const Status = injectSheet(styles)(StatusComponent);

export default Status;
