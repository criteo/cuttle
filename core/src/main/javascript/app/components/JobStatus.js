// @flow

import React from "react";
import { Badge } from "../components/Badge";

type Props = {
  status: string,
  labelFormatter?: string => string
};

export default function JobStatus({ status, labelFormatter = s => s }: Props) {
  switch (status) {
    case "running":
      return <Badge label={labelFormatter("RUNNING")} kind="info" width={75} />;
    case "throttled":
      return (
        <Badge
          label={labelFormatter("RETRYING")}
          kind="error"
          light={true}
          width={75}
        />
      );
    case "failed":
      return <Badge label={labelFormatter("FAILED")} kind="error" width={75} />;
    case "successful":
      return (
        <Badge label={labelFormatter("SUCCESS")} kind="success" width={75} />
      );
    case "paused":
      return <Badge label={labelFormatter("PAUSED")} width={75} light={true} />;
    case "waiting":
      return (
        <Badge label={labelFormatter("WAITING")} kind="warning" width={75} />
      );
    default:
      return <Badge label={labelFormatter(status)} width={75} />;
  }
}

JobStatus.displayName = "JobStatus";
