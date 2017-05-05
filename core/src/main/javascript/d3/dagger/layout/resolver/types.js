// @flow

import type {AnnotatedNode, AnnotatedEdge} from '../symbolic/annotatedGraph';

export interface ResolvedPoint {
  x: number;
  y: number;
}

export interface ResolvedNode extends AnnotatedNode {
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface ResolvedEdge extends AnnotatedNode {
  x1: number;
  y1: number;
  x2: number;
  y2: number;
}

export interface ResolvedGraphLayout {
  nodes: {[key: string]: ResolvedNode};
  edges: {[key: string]: ResolvedEdge};
}
