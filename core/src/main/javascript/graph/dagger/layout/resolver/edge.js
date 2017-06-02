// @flow
import type { AnnotatedGraph, AnnotatedNode } from "../symbolic/annotatedGraph";
import type { ResolvedNode } from "./types";
import { edgeKind, nodeKind } from "../symbolic/annotatedGraph";
import { ResolvedGraphLayout } from "./types";
import { GraphDimensions } from "../dimensions";
import _ from "lodash";
import * as d3 from "d3";

export const resolveEdgesForParentNode = (
  resolvedNode: $Shape<ResolvedNode>,
  annotatedNode: AnnotatedNode,
  centerNode: ResolvedNode,
  graph: AnnotatedGraph,
  scale: any
) =>
  graph
    .getEdges({ from: [annotatedNode.id] })
    .filter(p => p.kind === edgeKind.parentToCenter)
    .reduce(
      (acc, current) => ({
        ...acc,
        [current.id]: {
          x1: resolvedNode.x + resolvedNode.width / 2,
          y1: resolvedNode.y,
          x2: centerNode.x - centerNode.width / 2,
          y2: centerNode.y - centerNode.height / 2 + scale(resolvedNode.y),
          ...current
        }
      }),
      {}
    );

export const resolveEdgesForChildNode = (
  resolvedNode: $Shape<ResolvedNode>,
  annotatedNode: AnnotatedNode,
  centerNode: ResolvedNode,
  graph: AnnotatedGraph,
  scale: any
) =>
  graph
    .getEdges({ to: [annotatedNode.id] })
    .filter(p => p.kind === edgeKind.centerToChild)
    .reduce(
      (acc, current) => ({
        ...acc,
        [current.id]: {
          x1: centerNode.x + centerNode.width / 2,
          y1: centerNode.y - centerNode.height / 2 + scale(resolvedNode.y),
          x2: resolvedNode.x + resolvedNode.width / 2,
          y2: resolvedNode.y,
          ...current
        }
      }),
      {}
    );

export const resolveBorderEdges = (
  mainId: string,
  startPositions: { [key: string]: ResolvedNode },
  graphs: { [key: string]: AnnotatedGraph },
  dimensions: GraphDimensions,
  bigScale: any
) => {
  const allEdges = _.uniqBy(
    _.concat(...graphs[mainId].nodes.map(n => graphs[n.id].edges)),
    "id"
  );

  return allEdges.reduce((acc, current) => {
    const sourcePresent = current.source in startPositions;
    const targetPresent = current.target in startPositions;

    const sourceNode = sourcePresent ? current.source : current.target;
    const targetNode = targetPresent ? current.target : current.source;

    const kind = graphs[mainId].getEdgeKind(current.id);

    const sourceNodeStart = startPositions[sourceNode];
    const targetNodeStart = startPositions[targetNode];

    const sourceScale = bigScale(sourceNodeStart.height);
    const targetScale = bigScale(targetNodeStart.height);

    return {
      ...acc,
      [current.id]: {
        x1: sourceNodeStart.x + sourceNodeStart.width / 2,
        y1: sourceNodeStart.y -
          sourceNodeStart.height / 2 +
          sourceScale(targetNodeStart.y),
        x2: targetNodeStart.x + targetNodeStart.width / 2,
        y2: targetNodeStart.y -
          targetNodeStart.height / 2 +
          targetScale(sourceNodeStart.y),
        ...current,
        kind
      }
    };
  }, {});
};
