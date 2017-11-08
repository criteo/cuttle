import {
  transitionEdge,
  transitionNode,
  drawNode,
  drawEdge
} from "./nodesAndEdges";
import * as d3 from "d3";

import reduce from "lodash/reduce";
import drop from "lodash/drop";
import dropRight from "lodash/dropRight";

import * as minimapTools from "../minimap";
import { TransitionAction } from "../layout/timeMachine";

const mergeLayouts = layouts => ({
  nodes: reduce(layouts, (acc, current) => ({ ...acc, ...current.nodes }), {}),
  edges: reduce(layouts, (acc, current) => ({ ...acc, ...current.edges }), {})
});

// Moves needed to transit from a layout to another one, if the two graphs are disjoints
const computeStartEndDisconnectedLayout = (
  node,
  previousNode,
  allLayouts,
  minimap
) => {
  const currentNodePosition = minimap.getElementById(node).renderedPosition();
  const previousNodePosition = minimap
    .getElementById(previousNode)
    .renderedPosition();
  const switchOrder = {
    vertical: currentNodePosition.y >= previousNodePosition.y ? 1 : -1,
    horizontal: Math.abs(currentNodePosition.x - previousNodePosition.x) >
      0.15 * minimap.width()
      ? currentNodePosition.x >= previousNodePosition.x ? -1 : 1
      : 0
  };

  return {
    enterLayout: switchOrder.horizontal == 0
      ? allLayouts[node].shift(switchOrder.vertical == 1 ? "bottom" : "top")
      : allLayouts[node]
          .shift(switchOrder.vertical == 1 ? "bottom" : "top")
          .shift(switchOrder.horizontal == 1 ? "left" : "right"),
    exitLayout: switchOrder.horizontal == 0
      ? allLayouts[previousNode].shift(
          switchOrder.vertical == -1 ? "bottom" : "top"
        )
      : allLayouts[previousNode]
          .shift(switchOrder.vertical == -1 ? "bottom" : "top")
          .shift(switchOrder.horizontal == -1 ? "left" : "right")
  };
};

// Transition from one layout to another
export const transitionAction: TransitionAction = ({
  allEdgesContainer,
  allNodesContainer,
  tags,
  onClick
}) => (minimap, minimapOnClick) => ({
  layout,
  annotatedGraph,
  node,
  next,
  back,
  history,
  allLayouts,
  pathFrom,
  pathTo,
  previousNode
}) => {
  if (node.length === 0) return Promise.resolve("nothing");

  const resolvedTransition = transitionAction({
    allEdgesContainer,
    allNodesContainer,
    tags,
    onClick
  })(minimap, minimapOnClick);
  const currentNodesDom = allNodesContainer
    .selectAll("g.oneNode")
    .data(annotatedGraph[0].nodes, d => d.id);
  const currentEdgesDom = allEdgesContainer
    .selectAll("g.oneEdge")
    .data(annotatedGraph[0].edges, d => d.id);

  const onClickParameter = id => onClick(id, resolvedTransition, { next });
  const minimapOnClickParameter = id =>
    minimapOnClick(id, resolvedTransition, { next });

  minimap.nodes().off("click");
  minimap
    .nodes()
    .on("click", event => minimapOnClickParameter(event.cyTarget.id()));

  const minimapEnterPromise = minimapTools.enter(
    currentNodesDom.enter(),
    currentEdgesDom.enter(),
    minimap
  );
  const minimapUpdatePromise = minimapTools.update(
    currentNodesDom,
    currentEdgesDom,
    minimap
  );
  const minimapExitPromise = minimapTools.exit(
    currentNodesDom.exit(),
    currentEdgesDom.exit(),
    minimap
  );

  const {
    enterLayout: disconnectedEnterLayout,
    exitLayout: disconnectedExitLayout
  } = computeStartEndDisconnectedLayout(
    node[0],
    previousNode,
    allLayouts,
    minimap
  );

  // if node contains only one entry, it's because we try to reach a disconnected node (two disjoint graphs)
  const enterLayout = node.length >= 2
    ? mergeLayouts(drop(layout))
    : disconnectedEnterLayout;
  const exitLayout = node.length >= 2
    ? mergeLayouts(dropRight(layout).reverse())
    : disconnectedExitLayout;

  const updatePromise = update(
    currentNodesDom,
    currentEdgesDom,
    layout[0],
    onClickParameter
  );
  const enterPromise = enter(
    currentNodesDom.enter(),
    currentEdgesDom.enter(),
    enterLayout,
    layout[0],
    onClickParameter,
    tags
  );
  const exitPromise = exit(
    currentNodesDom.exit(),
    currentEdgesDom.exit(),
    exitLayout
  );

  return Promise.all([
    ...enterPromise,
    ...updatePromise,
    ...exitPromise,
    ...minimapEnterPromise,
    ...minimapUpdatePromise,
    ...minimapExitPromise
  ]);
};

//
//
// D3 Renderer enter update exit + drawing helpers
//
//

export const enter = (
  nodesSelection,
  edgesSelection,
  previousLayout,
  currentLayout,
  onClick,
  tags
) => {
  const promises = [];

  // Put entering nodes at the right place for a nice animation
  nodesSelection.each((d, i, nodes) =>
    promises.push(
      new Promise(resolve => {
        const node = drawNode(
          d3.select(nodes[i]),
          previousLayout.nodes[d.id],
          tags
        );
        node.on("click", () => onClick(d.id));
        transitionNode(node, currentLayout.nodes[d.id]).on("end", () =>
          resolve("enter nodes done")
        );
      })
    )
  );

  edgesSelection.each((d, i, nodes) =>
    promises.push(
      new Promise(resolve =>
        transitionEdge(
          drawEdge(
            d3.select(nodes[i]),
            previousLayout.edges[d.id],
            previousLayout.nodes
          ),
          currentLayout.edges[d.id],
          currentLayout.nodes
        ).on("end", () => resolve("enter edges done"))
      )
    )
  );

  return promises;
};

export const update = (
  nodesSelection,
  edgesSelection,
  currentLayout,
  onClick
) => {
  const promises = [];

  edgesSelection.each((d, i, nodes) =>
    promises.push(
      new Promise(resolve =>
        transitionEdge(
          d3.select(nodes[i]),
          currentLayout.edges[d.id],
          currentLayout.nodes
        ).on("end", () => resolve("update edges done"))
      )
    )
  );

  nodesSelection.each((d, i, nodes) =>
    promises.push(
      new Promise(resolve => {
        const node = d3.select(nodes[i]);
        node.on("click", () => onClick(d.id));
        transitionNode(node, currentLayout.nodes[d.id]).on("end", () =>
          resolve("update nodes done")
        );
      })
    )
  );

  return promises;
};

export const exit = (nodesSelection, edgesSelection, currentLayout) => {
  const promises = [];

  edgesSelection.each((d, i, nodes) =>
    promises.push(
      new Promise(resolve => {
        const movingEdge = transitionEdge(
          d3.select(nodes[i]),
          currentLayout.edges[d.id],
          currentLayout.nodes
        ).style("opacity", 0);
        movingEdge.on("end", () => resolve("exit edges done"));
        movingEdge.remove();
      })
    )
  );

  nodesSelection.each((d, i, nodes) =>
    promises.push(
      new Promise(resolve => {
        const movingNode = transitionNode(
          d3.select(nodes[i]),
          currentLayout.nodes[d.id]
        );
        movingNode.on("end", () => resolve("exit nodes done"));
        movingNode.remove();
      })
    )
  );

  return promises;
};
