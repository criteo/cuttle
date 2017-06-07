// @flow

import React from "react";
import { Badge } from "../components/Badge";

type Props = {
  status: string
};

export default ({ status }: Props) => {
  if (status == "running") {
    return <Badge label="RUNNING" kind="info" width={75} />;
  } else if (status == "throttled") {
    return <Badge label="STUCK" kind="error" light={true} width={75} />;
  } else if (status == "failed") {
    return <Badge label="FAILED" kind="error" width={75} />;
  } else if (status == "successful") {
    return <Badge label="SUCCESS" kind="success" width={75} />;
  } else if (status == "paused") {
    return <Badge label="PAUSED" width={75} light={true} />;
  } else if (status == "waiting") {
    return <Badge label="WAITING" kind="warning" width={75} />;
  } else {
    return <Badge label={status} width={75} />;
  }
};
