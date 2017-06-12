// @flow
import {
  AnnotatedNode,
  nodeKind,
  AnnotatedGraph
} from "../../symbolic/annotatedGraph";
import { GraphDimensions } from "../../dimensions";

import map from "lodash/map";
import reduce from "lodash/reduce";
import filter from "lodash/filter";
import uniqBy from "lodash/uniqBy";
import orderBy from "lodash/orderBy";

const sign = n => (n === 0 ? 1 : n / Math.abs(n));

const relativeSlotsCounter = () => {
  let above = 1;
  let under = 1;
  return n => (n > 0 ? under++ : above++);
};

export const resolveStartPositions = (
  parents: AnnotatedNode[],
  children: AnnotatedNode[],
  graphs: { [key: string]: AnnotatedGraph },
  dimensions: GraphDimensions
) => {
  const childNodeDimensions = dimensions.nodeSize(children.length);
  const parentNodeDimensions = dimensions.nodeSize(parents.length);
  // Parents
  const parentsOfParents = reduce(
    parents,
    (acc, current) => {
      const pOp = graphs[current.id].findNodesByTag(nodeKind.parent);
      return [...acc, ...pOp];
    }, []);
  const parentOfParentsPositions = reduce(
    orderBy(uniqBy(parentsOfParents, "id"),"order"),
    (acc, current, i, array) => {
      const x = -dimensions.normalNode.width * 2;
      const y = dimensions.nodeVerticalOffset(i, array.length);
      return {
        ...acc,
        [current.id]: { x, y, ...parentNodeDimensions, ...current }
      };
    }, {});

  const childrenOfParents = reduce(parents, (acc, current) => {
    const cOp: any = map(
      graphs[current.id].findNodesByTag(nodeKind.child),
      n => ({
        ...n,
        yRelativePosition: sign(n.yPosition - current.yPosition)
      }));
    return [...acc, ...cOp];
  }, []);
  const relPosCnt = relativeSlotsCounter();
  const childrenOfParentsPositions = reduce(
    orderBy(uniqBy(childrenOfParents, "id"), "order"),
    (acc, current) => {
      const x = dimensions.grid.childOffset + dimensions.normalNode.width / 2;
      const y =
        dimensions.canva.height / 2 +
          current.yRelativePosition *
      (dimensions.canva.height / 2 +
        relPosCnt(current.yRelativePosition) * dimensions.normalNode.height) +
          dimensions.normalNode.height / 2;
      return {
        ...acc,
        [current.id]: { x, y, ...childNodeDimensions, ...current }
      };
    }, {});

  // Children
  const childrenOfChildren = reduce(
    children,
    (acc, current) => {
      const cOc = graphs[current.id].findNodesByTag(nodeKind.child);
      return [...acc, ...cOc];
    }, []);
  const childrenOfChildrenPositions = reduce(
    orderBy(uniqBy(childrenOfChildren, "id"), "order"),
    (acc, current, i, array) => {
      const x = dimensions.canva.width + dimensions.normalNode.width / 2;
      const y = dimensions.nodeVerticalOffset(i, array.length);
      return {
        ...acc,
        [current.id]: { x, y, ...childNodeDimensions, ...current }
      };
    }, {});

  const parentsOfChildren = reduce(
    children,
    (acc, current) => {
      const pOc: any = map(
        graphs[current.id].findNodesByTag(nodeKind.parent),
        n => ({
          ...n,
          yRelativePosition: sign(n.yPosition - current.yPosition)
        }));
      return [...acc, ...pOc];
    }, []);
  const relPosCnt2 = relativeSlotsCounter();
  const parentsOfChildrenPositions = reduce(
    orderBy(uniqBy(parentsOfChildren, "id"), "order"),
    (acc, current) => {
      const x = dimensions.grid.parentOffset + dimensions.normalNode.width / 2;
      const y =
        dimensions.canva.height / 2 +
          current.yRelativePosition *
      (dimensions.canva.height / 2 +
        relPosCnt2(current.yRelativePosition) *
        dimensions.normalNode.height) +
          dimensions.normalNode.height / 2;
      return {
        ...acc,
        [current.id]: { x, y, ...parentNodeDimensions, ...current }
      };
    }, {});

  // When a node is child of a rendered parent, and parent of a rendered child, it should be laid out between them (x) in the hidden space
  const middleNodes = filter(
    parentsOfChildrenPositions,
    p => p.id in childrenOfParentsPositions
  );

  return {
    ...parentOfParentsPositions,
    ...childrenOfChildrenPositions,
    ...parentsOfChildrenPositions,
    ...childrenOfParentsPositions,
    ...reduce(
      middleNodes,
      (acc, current) => ({
        ...acc,
        [current.id]: { ...current, x: dimensions.grid.mainX }
      }),
      {}
    )
  };
};
