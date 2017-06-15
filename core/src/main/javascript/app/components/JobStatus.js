// @flow

import React from "react";
import { Badge } from "../components/Badge";

type Props = {
  status: string
};

export default function JobStatus({ status }: Props) {
  switch (status) {
    case "running":
      return <Badge label="RUNNING" kind="info" width={75} />;
    case "throttled":
      return <Badge label="RETRYING" kind="error" light={true} width={75} />;
    case "failed":
      return <Badge label="FAILED" kind="error" width={75} />;
    case "successful":
      return <Badge label="SUCCESS" kind="success" width={75} />;
    case "paused":
      return <Badge label="PAUSED" width={75} light={true} />;
    case "waiting":
      return <Badge label="WAITING" kind="warning" width={75} />;
    default:
      return <Badge label={status} width={75} />;
  }
}

JobStatus.displayName = "JobStatus";
