// @flow
import { ResolvedGraphLayout } from "./resolver/types";
import { AnnotatedGraph, nodeKind } from "./symbolic/annotatedGraph";
import { Graph } from "../dataAPI/genericGraph";

import { resolveFixedNodes } from "./resolver/fixed";
import { resolveStartPositions } from "./resolver/node/start";
import { resolveBorderEdges } from "./resolver/edge";

import { GraphDimensions } from "./dimensions";

import max from "lodash/max";
import reduce from "lodash/reduce";
import map from "lodash/map";
import mapValues from "lodash/mapValues";
import values from "lodash/values";
import orderBy from "lodash/orderBy";
import memoize from "lodash/memoize";

import { scalePow } from "d3";

type AnnotatedGraphMap = { [mainNodeId: string]: AnnotatedGraph };
type FixLayoutMap = { [mainNodeId: string]: ResolvedGraphLayout };

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
    nodes: mapValues(layout.nodes, shiftNodes),
    edges: mapValues(layout.edges, shiftEdges)
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

  const graphs = reduce(
    overallGraph.nodes,
    (acc, current) => ({
      ...acc,
      [current.id]: AnnotatedGraph.build(current.id, overallGraph)
    }),
    {}
  );

  return reduce(
    overallGraph.nodes,
    (acc: LayoutManager, current) => {
      const graph = graphs[current.id];
      const parents = orderBy(graph.findNodesByTag(nodeKind.parent), "order");
      const children = orderBy(graph.findNodesByTag(nodeKind.child), "order");

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
        max(
          map(values(startNodePositions), n =>
            Math.abs(n.y - dimensions.canva.height / 2)
          )
        );
      const bigScale = memoize(height =>
        scalePow()
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
    },
    lm
  );
};
