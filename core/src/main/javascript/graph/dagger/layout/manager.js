// @flow
import { ResolvedGraphLayout } from "./resolver/types";
import type { NodeKind } from "./symbolic/annotatedGraph";
import { AnnotatedGraph, nodeKind } from "./symbolic/annotatedGraph";
import { Graph } from "../dataAPI/genericGraph";

import { resolveFixedNodes } from "./resolver/fixed";
import { resolveStartPositions } from "./resolver/node/start";
import { resolveBorderEdges } from "./resolver/edge";

import { GraphDimensions } from "./dimensions";
import _ from "lodash";
import * as d3 from "d3";

type AnnotatedGraphMap = { [mainNodeId: string]: AnnotatedGraph };
type FixLayoutMap = { [mainNodeId: string]: ResolvedGraphLayout };
type StartLayoutMap = { [selectedNodeId: string]: ResolvedGraphLayout };

export interface LayoutManager {
  annotatedGraph: $Shape<AnnotatedGraphMap>,
  layout: $Shape<FixLayoutMap>
}

const shiftLayout = (
  dimensions: GraphDimensions,
  layout: $Shape<ResolvedGraphLayout>
) => (direction: string) => {
  let shiftNodes, shiftEdges;
  switch (direction) {
    case "left":
      shiftNodes = n => ({ ...n, x: n.x - dimensions.canva.width });
      shiftEdges = n => ({
        ...n,
        x1: n.x1 - dimensions.canva.width,
        x2: n.x2 - dimensions.canva.width
      });
      break;
    case "right":
      shiftNodes = n => ({ ...n, x: n.x + dimensions.canva.width });
      shiftEdges = n => ({
        ...n,
        x1: n.x1 + dimensions.canva.width,
        x2: n.x2 + dimensions.canva.width
      });
      break;
    case "top":
      shiftNodes = n => ({ ...n, y: n.y - dimensions.canva.height });
      shiftEdges = n => ({
        ...n,
        y1: n.y1 - dimensions.canva.height,
        y2: n.y2 - dimensions.canva.height
      });
      break;
    case "bottom":
      shiftNodes = n => ({ ...n, y: n.y + dimensions.canva.height });
      shiftEdges = n => ({
        ...n,
        y1: n.y1 + dimensions.canva.height,
        y2: n.y2 + dimensions.canva.height
      });
      break;
    default:
      shiftNodes = n => n;
      shiftEdges = n => n;
  }

  const myLayout = {
    nodes: _.mapValues(layout.nodes, shiftNodes),
    edges: _.mapValues(layout.edges, shiftEdges)
  };
  return { ...myLayout, shift: shiftLayout(dimensions, myLayout) };
};

export const buildCachedLayoutManager = (
  overallGraph: Graph,
  dimensions: GraphDimensions
) => {
  const lm: LayoutManager = {
    annotatedGraph: {},
    layout: {}
  };

  const graphs = overallGraph.nodes.reduce(
    (acc, current) => ({
      ...acc,
      [current.id]: AnnotatedGraph.build(current.id, overallGraph)
    }),
    {}
  );
  return overallGraph.nodes.reduce((acc: LayoutManager, current) => {
    const graph = graphs[current.id];
    const parents = _.orderBy(graph.findNodesByTag(nodeKind.parent), "order");
    const children = _.orderBy(graph.findNodesByTag(nodeKind.child), "order");

    const centralLayout = resolveFixedNodes(
      parents,
      children,
      graph,
      dimensions
    );
    const borderNodes = resolveStartPositions(
      parents,
      children,
      graphs,
      dimensions
    );

    const startNodePositions = {
      ...borderNodes,
      ...centralLayout.nodes
    };

    const yMaxPosition =
      1.30 *
      _.max(
        _.values(startNodePositions).map(n =>
          Math.abs(n.y - dimensions.canva.height / 2)
        )
      );
    const bigScale = _.memoize(height =>
      d3
        .scalePow()
        .domain([
          dimensions.canva.height / 2 - yMaxPosition,
          dimensions.canva.height / 2 + yMaxPosition
        ])
        .range([0, height])
        .clamp(true)
    );

    const startEdgesPositions = {
      ...resolveBorderEdges(
        current.id,
        startNodePositions,
        graphs,
        dimensions,
        bigScale
      ),
      ...centralLayout.edges
    };

    acc.annotatedGraph[current.id] = graph;
    const layout = { nodes: startNodePositions, edges: startEdgesPositions };
    acc.layout[current.id] = {
      ...layout,
      shift: shiftLayout(dimensions, layout)
    };
    return acc;
  }, lm);
};
