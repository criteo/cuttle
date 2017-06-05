//@flow
import * as minimapTools from "./minimap";
import { Graph } from "./dataAPI/genericGraph";

import { GraphDimensions } from "./layout/dimensions";
import { buildCachedLayoutManager } from "./layout/manager";
import { timeMachineGenerator } from "./layout/timeMachine";
import { transitionAction } from "./render/d3render";
import noop from "lodash/noop";

import * as d3 from "d3";

const defaultOptions = {
  width: undefined,
  height: undefined,
  nodesContainer: undefined,
  edgesContainer: undefined,
  tags: {},
  onClickNode: noop,
  startNodeId: undefined,
  startHistory: [],
  minimap: {
    onClickNode: noop,
    container: undefined,
    setup: noop
  }
};

export const buildDagger = (overallGraph: Graph, userOptions: any = {}) => {
  // Options preparation
  const { minimap: userMinimapOptions = {} } = userOptions;
  const minimapOptions = { ...defaultOptions.minimap, ...userMinimapOptions };
  const options = { ...defaultOptions, ...userOptions };

  const minimap = minimapTools.draw(overallGraph, minimapOptions.container);
  overallGraph.changeNodesOrder(minimapTools.getNodesOrder(minimap));

  const { width, height } = options;
  const dimensions = GraphDimensions.buildDefaultDimensions({ width, height });

  const layoutManager = buildCachedLayoutManager(overallGraph, dimensions);
  const timeMachine = timeMachineGenerator(
    layoutManager,
    options.startHistory,
    options.startNodeId || overallGraph.nodes[0].id,
    minimap
  );
  const onClickDefault = (id, resolvedTransition, { next }) =>
    next(id, resolvedTransition);
  
  const trAction = transitionAction({
    allNodesContainer: d3.select(options.nodesContainer),
    allEdgesContainer: d3.select(options.edgesContainer),
    tags: options.tags,
    onClick: options.onClickNode || onClickDefault
  })(minimap, minimapOptions.onClickNode || onClickDefault);

  minimapOptions.setup(minimap);

  return {
    updateDimensions: (width: number, height: number) =>
      buildDagger(overallGraph, {
        ...options,
        width,
        height
      }),
    initRender: () => timeMachine(trAction),
    transitionAction: (nodesContainer: any, edgesContainer: any) => transitionAction({
      allNodesContainer: d3.select(nodesContainer),
      allEdgesContainer: d3.select(edgesContainer),
      tags: options.tags,
      onClick: options.onClickNode || onClickDefault
    })(minimap, minimapOptions.onClickNode || onClickDefault)
  };
};
