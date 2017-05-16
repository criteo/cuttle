// @flow

import React from "react";
import type { NodeKind } from "../datamodel/workflow";

type Props = {
  kind?: NodeKind
};

const size = 1;
const radius = 6 * size;
const strokeWidth = 2 * size;

type NodeProps = {
  radius: number,
  strokeWidth: number,
  color?: string
};

const RootNode = ({ radius, strokeWidth, color }: NodeProps) => (
  <svg
    width={2 * radius + 2 * strokeWidth}
    height={3 * radius + 2 * strokeWidth}
  >
    <circle
      stroke={color}
      strokeWidth={strokeWidth}
      r={radius}
      fill={color}
      cx={radius + strokeWidth}
      cy={radius + strokeWidth}
    />
    <path
      d={
        "M" + (radius + strokeWidth) + "," + (radius*2 + strokeWidth) +
        "L" + (radius + strokeWidth) + "," + (radius*3 + 2*strokeWidth)}
      stroke={color} strokeWidth={strokeWidth} fill="none" />
  </svg>
);

const LeafNode = ({ radius, strokeWidth, color }: NodeProps) => (
  <svg
    width={2*radius + 2*strokeWidth}
    height={3*radius + 2*strokeWidth}
  >
    <circle
      stroke={color}
      strokeWidth={strokeWidth}
      r={radius}
      fill={color}
      cx={radius + strokeWidth}
      cy={2*radius + strokeWidth}
    />
    <path d={"M" + (radius + strokeWidth) + "," + 0 +
      "L" + (radius + strokeWidth) + "," + (radius + strokeWidth)}
      stroke={color} strokeWidth={strokeWidth} fill="none" />
  </svg>
);

const CommonNode = ({ radius, strokeWidth, color }: NodeProps) => (
  <svg
    width={2*radius + 2*strokeWidth}
    height={3*radius + 2*strokeWidth}
  >
    <circle
      stroke={color}
      strokeWidth={strokeWidth}
      r={radius}
      fill="none"
      cx={radius + strokeWidth}
      cy={1.5*radius + strokeWidth}
    />
    <path d={"M" + (radius + strokeWidth) + "," + 0 +
      "L" + (radius + strokeWidth) + "," + (0.5*radius + strokeWidth)}
      stroke={color} strokeWidth={strokeWidth} fill="none" />
    <path d={"M" + (radius + strokeWidth) + "," + (radius*2.5 + strokeWidth) +
      "L" + (radius + strokeWidth) + "," + (radius*3 + 2*strokeWidth)}
      stroke={color} strokeWidth={strokeWidth} fill="none" />
  </svg>
);

const NodeIcon = ({ kind = "common" }: Props) =>
  kind == "root"
  ? <RootNode size={size} radius={radius} strokeWidth={strokeWidth} color="#2F3647"/>
  : kind == "leaf"
  ? <LeafNode size={size} radius={radius} strokeWidth={strokeWidth} color="#7D8B99"/>
  : <CommonNode size={size} radius={radius} strokeWidth={strokeWidth} color="#7D8B99"/>;

export default NodeIcon;
