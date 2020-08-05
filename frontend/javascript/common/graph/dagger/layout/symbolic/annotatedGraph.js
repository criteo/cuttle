//@flow
import { Node, Edge, Graph } from "../../dataAPI/genericGraph";
import _ from "lodash";

export const nodeKind = {
  parent: "nodeKind_parent",
  child: "nodeKind_child",
  main: "nodeKind_main"
};

export type NodeKind = "nodeKind_parent" | "nodeKind_child" | "nodeKind_main";

export const edgeKind = {
  parentToCenter: "edgeKind_parentToCenter",
  centerToChild: "edgeKind_centerToChild",
  parentToMissingChild: "edgeKind_parentToMissingChild",
  missingParentToChild: "edgeKind_missingParentToChild",
  childToMissingChild: "edgeKind_childToMissingChild",
  missingParentToParent: "edgeKind_missingParentToParent",
  border: "edgeKind_border"
};

export type EdgeKind =
  | "edgeKind_parentToCenter"
  | "edgeKind_centerToChild"
  | "edgeKind_parentToMissingChild"
  | "edgeKind_missingParentToChild"
  | "edgeKind_childToMissingChild"
  | "edgeKind_missingParentToParent"
  | "edgeKind_border";

export interface AnnotatedNode extends Node {
  kind: NodeKind;
}

export interface AnnotatedEdge extends Edge {
  kind: EdgeKind;
}

// Subgraph around the selected node Id, with symbolic layout
export class AnnotatedGraph {
  nodes: AnnotatedNode[];
  edges: AnnotatedEdge[];
  constructor(nodes: AnnotatedNode[], edges: AnnotatedEdge[]) {
    this.nodes = nodes;
    this.edges = edges;
  }

  findNodesByTag(kind: NodeKind): AnnotatedNode[] {
    return this.nodes.filter(n => n.kind === kind);
  }

  getNode(id: string) {
    return this.nodes.find(n => n.id === id);
  }

  getEdgeKind(id: string): EdgeKind {
    const edge = this.edges.find(n => n.id === id);
    return edge ? edge.kind : edgeKind.border;
  }

  getEdges({
    from,
    to
  }: $Shape<{ from: string[], to: string[] }>): AnnotatedEdge[] {
    const filterSource =
      typeof from === "undefined"
        ? edge => true
        : edge => from.includes(edge.source);
    const filterTarget =
      typeof to === "undefined"
        ? edge => true
        : edge => to.includes(edge.target);
    return this.edges.filter(e => filterSource(e) && filterTarget(e));
  }

  static build(nodeId: string, graphDB: Graph): AnnotatedGraph {
    const mainOriginalNode = graphDB.getNode(nodeId) || {
      id: nodeId,
      order: 0,
      yPosition: 0
    };
    const parents = graphDB.getParents(nodeId);
    const children = graphDB.getChildren(nodeId);
    const parentIds: string[] = parents.map(p => p.id);
    const childrenIds: string[] = children.map(c => c.id);

    const parentNodes: AnnotatedNode[] = parents.map((el, i) => ({
      ...el,
      kind: nodeKind.parent
    }));
    const childNodes: AnnotatedNode[] = children.map((el, i) => ({
      ...el,
      kind: nodeKind.child
    }));
    const mainNode: AnnotatedNode = {
      ...mainOriginalNode,
      kind: nodeKind.main
    };

    const parentToCenterLinks: AnnotatedEdge[] = graphDB
      .getEdges({ from: parentIds, to: [nodeId] })
      .map(e => ({ ...e, kind: edgeKind.parentToCenter }));
    const centerToChildLinks: AnnotatedEdge[] = graphDB
      .getEdges({ from: [nodeId], to: childrenIds })
      .map(e => ({ ...e, kind: edgeKind.centerToChild }));
    const parentToMissingChildLinks: AnnotatedEdge[] = graphDB
      .getEdges({ from: parentIds })
      .filter(e => e.target !== nodeId)
      .map(e => ({ ...e, kind: edgeKind.parentToMissingChild }));
    const missingParentToChildLinks: AnnotatedEdge[] = graphDB
      .getEdges({ to: childrenIds })
      .filter(e => e.source !== nodeId)
      .map(e => ({ ...e, kind: edgeKind.missingParentToChild }));
    const childToMissingChildLinks: AnnotatedEdge[] = graphDB
      .getEdges({ from: childrenIds })
      .map(e => ({ ...e, kind: edgeKind.childToMissingChild }));
    const missingParentToParentLinks: AnnotatedEdge[] = graphDB
      .getEdges({ to: parentIds })
      .map(e => ({ ...e, kind: edgeKind.missingParentToParent }));

    return new AnnotatedGraph(
      [...parentNodes, ...childNodes, mainNode],
      [
        ...parentToCenterLinks,
        ...centerToChildLinks,
        ...parentToMissingChildLinks,
        ...missingParentToChildLinks,
        ...childToMissingChildLinks,
        ...missingParentToParentLinks
      ]
    );
  }
}
