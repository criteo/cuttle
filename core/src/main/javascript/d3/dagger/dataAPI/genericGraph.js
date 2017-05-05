// @flow
import includes from "lodash/includes";
import isUndefined from "lodash/isUndefined";
import constant from "lodash/constant";
import filter from "lodash/filter";
import find from "lodash/find";
import overEvery from "lodash/overEvery";

export interface Node {
  id: string;
  order: number;
  name?: string | typeof undefined;
  description?: string | typeof undefined;
  tags?: string[] | typeof undefined;
  yPosition: number;
}

export interface Edge {
  id: string;
  source: string;
  target: string;
  value: number;
}

export class Graph {
  nodes: Node[];
  edges: Edge[];
  constructor(nodes: Node[], edges: Edge[]) {
    this.nodes = nodes;
    this.edges = edges;
  }

  getNode(nodeId: string): ?Node {
    return find(this.nodes, {id: nodeId});
  }

  getParents(nodeId: string): Node[]{
    return filter(this.nodes, n => filter(this.edges, {source: n.id, target: nodeId}).length > 0);
  }

  getChildren(nodeId: string): Node[]{
    return filter(this.nodes, n => filter(this.edges, {source: nodeId, target: n.id}).length > 0);
  }

  getEdges({from, to}: $Shape<{from: string[], to: string[]}>): Edge[] {
    const filterSource = isUndefined(from) ? constant(true) : (edge => includes(from, edge.source));
    const filterTarget = isUndefined(to) ? constant(true) : (edge => includes(to, edge.target));
    return filter(this.edges, overEvery([filterSource, filterTarget]));
  }

  changeNodesOrder(orderedIds: Node[]): void {
    orderedIds.forEach(el => {
      const node = find(this.nodes, {id: el.id});
      if(node) node.order = el.order, node.yPosition = el.yPosition;
    })
  }
}
