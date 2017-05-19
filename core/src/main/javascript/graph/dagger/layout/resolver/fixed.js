// @flow

import {
  AnnotatedGraph,
  AnnotatedNode,
  nodeKind
} from "../symbolic/annotatedGraph";
import { GraphDimensions } from "../dimensions";
import type { ResolvedGraphLayout, ResolvedEdge } from "./types";
import { resolveEdgesForChildNode, resolveEdgesForParentNode } from "./edge";
import * as d3 from "d3";

const scaleBuilder = (fullHeight, nodeHeight) =>
  d3.scaleLinear().domain([0, fullHeight]).range([0, nodeHeight]);

export const resolveFixedNodes = (
  parents: AnnotatedNode[],
  children: AnnotatedNode[],
  graph: AnnotatedGraph,
  dimensions: GraphDimensions
): ResolvedGraphLayout => {
  const {
    width: parentNodeWidth,
    height: parentNodeHeight
  } = dimensions.nodeSize(parents.length);
  const {
    width: childNodeWidth,
    height: childNodeHeight
  } = dimensions.nodeSize(children.length);
  const mainNode = graph.findNodesByTag(nodeKind.main);
  const main = mainNode.length > 0
    ? mainNode[0]
    : { id: "unknown", order: -1, yPosition: -1, kind: nodeKind.main };

  let edges: { [key: string]: ResolvedEdge } = {};
  const { width: mainWidth, height: mainHeight } = dimensions.nodeSize(0);
  // The scale is here necessary to put different start/end positions for the edges joining/quitting the main node
  const scale = scaleBuilder(dimensions.canva.height, mainHeight);
  const mainPosition = {
    width: mainWidth,
    height: mainHeight,
    x: dimensions.canva.width / 2,
    y: dimensions.canva.height / 2,
    ...main
  };

  const parentPositions = parents.reduce(
    (acc, current, i) => {
      const x = dimensions.parentOffset + parentNodeWidth / 2;
      const y = dimensions.nodeVerticalOffset(i, parents.length);
      const newNodePosition = {
        x,
        y,
        width: parentNodeWidth,
        height: parentNodeHeight,
        ...current
      };
      edges = {
        ...edges,
        ...resolveEdgesForParentNode(
          newNodePosition,
          current,
          mainPosition,
          graph,
          scale
        )
      };
      return { ...acc, [current.id]: newNodePosition };
    },
    {}
  );

  const childrenPositions = children.reduce(
    (acc, current, i) => {
      const x = dimensions.childOffset + childNodeWidth / 2;
      const y = dimensions.nodeVerticalOffset(i, children.length);
      const newNodePosition = {
        x,
        y,
        width: childNodeWidth,
        height: childNodeHeight,
        ...current
      };
      edges = {
        ...edges,
        ...resolveEdgesForChildNode(
          newNodePosition,
          current,
          mainPosition,
          graph,
          scale
        )
      };
      return { ...acc, [current.id]: newNodePosition };
    },
    {}
  );

  return {
    nodes: {
      ...parentPositions,
      ...childrenPositions,
      [main.id]: mainPosition
    },
    edges
  };
};
