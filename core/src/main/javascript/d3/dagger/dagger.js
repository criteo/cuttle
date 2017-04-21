//@flow
import * as d3render from './render/d3render';
import * as minimapTools from './minimap';
import {Graph} from './dataAPI/genericGraph';

import {GraphDimensions} from './layout/dimensions';
import {buildCachedLayoutManager} from './layout/manager';
import {timeMachineGenerator} from './layout/timeMachine';
import {initContainers, transitionAction} from './render/d3render';

import * as d3 from "d3";

const defaultOptions = {
  dimensions: GraphDimensions.buildDefaultDimensions(),
  nodesContainer: undefined,
  edgesContainer: undefined,
  startNodeId: undefined,
  startHistory: [],
  minimap: {
    container: d3.select("#minimap-container"),
    setup: () => {}
  }
};

export const buildDagger = (overallGraph: Graph, userOptions: any = {}) => {
  // Options preparation
  const {minimap:userMinimapOptions = {}} = userOptions;
  const minimapOptions = {...defaultOptions.minimap, ...userMinimapOptions};
  const options = {...defaultOptions, ...userOptions};

  const minimap = minimapTools.draw(overallGraph, minimapOptions.container);
  overallGraph.changeNodesOrder(minimapTools.getNodesOrder(minimap));
  
  const layoutManager = buildCachedLayoutManager(overallGraph, options.dimensions);
  const timeMachine = timeMachineGenerator(layoutManager, options.startHistory, options.startNodeId || overallGraph.nodes[0].id, minimap);
  const trAction = transitionAction({
    allNodesContainer: d3.select(options.nodesContainer),
    allEdgesContainer: d3.select(options.edgesContainer)
  })(minimap);

  minimapOptions.setup(minimap);

  return {
    initRender: () => timeMachine(trAction),
  };
};
