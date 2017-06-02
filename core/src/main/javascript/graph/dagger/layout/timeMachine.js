// @flow

import { LayoutManager } from "./manager";
import { ResolvedGraphLayout } from "./resolver/types";
import { AnnotatedGraph } from "./symbolic/annotatedGraph";

import compact from "lodash/compact";
import take from "lodash/take";
import map from "lodash/map";

export type TransitionAction = (
  arg: $Shape<{
    layout: ResolvedGraphLayout[],
    annotatedGraph: AnnotatedGraph[],
    node: string[],
    next: (nodeId: string, action: TransitionAction) => Promise<NextReturn>,
    back: (relPos: number, action: TransitionAction) => Promise<NextReturn>,
    history: (relPos: number) => string[],
    allLayouts: { [key: string]: ResolvedGraphLayout },
    pathFrom: any,
    pathTo: any,
    previousNode: string
  }>
) => Promise<*>;

export interface TimeMachine {
  node: string,
  next: (a: string, b: TransitionAction) => Promise<NextReturn>,
  back: (a: number, b: TransitionAction) => Promise<NextReturn>,
  history: (relPos: number) => string[]
}

export interface NextReturn {
  value: any,
  timeMachine: TimeMachine
}

const computeDijkstraPath = (minimap, start) => {
  const path = minimap.elements().dijkstra(minimap.getElementById(start));
  return end =>
    compact(
      map(
        path.pathTo(minimap.getElementById(end)),
        n => n.isNode() ? n.id() : null
      )
    );
};

export const timeMachineGenerator = (
  lm: LayoutManager,
  history: string[],
  currentNode: string,
  minimap: any
) => {
  const historyAccess = historyBuilder(history);
  const next = nextBuilder(lm, currentNode, history, minimap);
  const back = backBuilder(lm, currentNode, history, minimap);
  const timeMachine = {
    node: currentNode,
    history: historyAccess,
    next,
    back
  };
  return (init: TransitionAction = () => Promise.resolve({})) =>
    init({
      layout: [lm.layout[currentNode], lm.layout[currentNode]],
      annotatedGraph: [
        lm.annotatedGraph[currentNode],
        lm.annotatedGraph[currentNode]
      ],
      node: [currentNode, currentNode],
      history: historyAccess,
      back,
      next,
      allLayouts: lm.layout,
      previousNode: currentNode
    }).then(value => ({ value, timeMachine }));
};

const nextBuilder = (
  lm: LayoutManager,
  currentNode: string,
  history: string[],
  minimap: any
) => {
  const nextHistory = [currentNode, ...history];
  const nextHistoryAccess = historyBuilder(nextHistory);
  const unresolvedPath = computeDijkstraPath(minimap, currentNode);
  return (nodeId: string, action: TransitionAction): Promise<NextReturn> => {
    const next = nextBuilder(lm, nodeId, nextHistory, minimap);
    const back = backBuilder(lm, nodeId, nextHistory, minimap);
    const finalPath = unresolvedPath(nodeId).reverse();
    const nextTimeMachine = {
      node: nodeId,
      history: nextHistoryAccess,
      next,
      back
    };
    return action({
      layout: map(finalPath, n => lm.layout[n]),
      node: finalPath,
      annotatedGraph: map(finalPath, n => lm.annotatedGraph[n]),
      history: nextHistoryAccess,
      next,
      back,
      allLayouts: lm.layout,
      pathFrom: computeDijkstraPath(minimap, nodeId),
      pathTo: unresolvedPath,
      previousNode: currentNode
    }).then(value => ({ value, timeMachine: nextTimeMachine }));
  };
};

const backBuilder = (
  lm: LayoutManager,
  currentNode: string,
  history: string[],
  minimap: any
) => {
  const [previousNode, ...previousHistory] = history;
  const previousHistoryAccess = historyBuilder(previousHistory);
  const unresolvedPath = computeDijkstraPath(minimap, currentNode);
  return (
    relPos: number = 1,
    action: TransitionAction
  ): Promise<NextReturn> => {
    const next = nextBuilder(lm, previousNode, previousHistory, minimap);
    const back = backBuilder(lm, previousNode, previousHistory, minimap);
    const finalPath = unresolvedPath(previousNode).reverse();
    const previousTimeMachine = {
      node: previousNode,
      history: previousHistoryAccess,
      next,
      back
    };
    return action({
      layout: map(finalPath, n => lm.layout[n]),
      node: finalPath,
      annotatedGraph: map(finalPath, n => lm.annotatedGraph[n]),
      history: previousHistoryAccess,
      next,
      back,
      allLayouts: lm.layout,
      pathFrom: computeDijkstraPath(minimap, previousNode),
      pathTo: unresolvedPath,
      previousNode: currentNode
    }).then(value => ({ value, timeMachine: previousTimeMachine }));
  };
};

const historyBuilder = (history: string[]) =>
  (relPos: number = 3): string[] => take(history, relPos);
