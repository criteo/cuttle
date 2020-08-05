// @flow

import React from "react";
import { Badge } from "../../common/components/Badge";

type Props = {
  status: string
};

export default function BackfillStatus({ status }: Props) {
  switch (status) {
    case "RUNNING":
      return <Badge label="RUNNING" kind="alt" width={75} />;
    case "COMPLETE":
      return <Badge label="COMPLETE" kind="success" width={75} />;
    default:
      return <Badge label={status} width={75} />;
  }
}

BackfillStatus.displayName = "Backfill";
