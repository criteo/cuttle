// @flow
import _ from "lodash";
import {
  AnnotatedNode,
  nodeKind,
  AnnotatedGraph
} from "../../symbolic/annotatedGraph";
import type { Node } from "../../../dataAPI/genericGraph";
import { LayoutManager } from "../../manager";
import { GraphDimensions } from "../../dimensions";

const sign = n => n === 0 ? 1 : n / Math.abs(n);

const relativeSlotsCounter = () => {
  let above = 1;
  let under = 1;
  return n => n > 0 ? under++ : above++;
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
  const parentsOfParents = parents.reduce(
    (acc, current) => {
      const pOp = graphs[current.id].findNodesByTag(nodeKind.parent);
      return [...acc, ...pOp];
    },
    []
  );
  const parentOfParentsPositions = _.orderBy(
    _.uniqBy(parentsOfParents, "id"),
    "order"
  ).reduce(
    (acc, current, i, array) => {
      const x = -dimensions.normalNode.width * 2;
      const y = dimensions.nodeVerticalOffset(i, array.length);
      return {
        ...acc,
        [current.id]: { x, y, ...parentNodeDimensions, ...current }
      };
    },
    {}
  );

  const childrenOfParents = parents.reduce(
    (acc, current) => {
      const cOp: any = graphs[current.id]
        .findNodesByTag(nodeKind.child)
        .map(n => ({
          ...n,
          yRelativePosition: sign(n.yPosition - current.yPosition)
        }));
      return [...acc, ...cOp];
    },
    []
  );
  const relPosCnt = relativeSlotsCounter();
  const childrenOfParentsPositions = _.orderBy(
    _.uniqBy(childrenOfParents, "id"),
    "order"
  ).reduce(
    (acc, current, i, array) => {
      const x = dimensions.childOffset + dimensions.normalNode.width / 2;
      const y = dimensions.canva.height / 2 +
        current.yRelativePosition *
          (dimensions.canva.height / 2 +
            relPosCnt(current.yRelativePosition) *
              dimensions.normalNode.height) +
        dimensions.normalNode.height / 2;
      return {
        ...acc,
        [current.id]: { x, y, ...childNodeDimensions, ...current }
      };
    },
    {}
  );

  // Children
  const childrenOfChildren = children.reduce(
    (acc, current) => {
      const cOc = graphs[current.id].findNodesByTag(nodeKind.child);
      return [...acc, ...cOc];
    },
    []
  );
  const childrenOfChildrenPositions = _.orderBy(
    _.uniqBy(childrenOfChildren, "id"),
    "order"
  ).reduce(
    (acc, current, i, array) => {
      const x = dimensions.canva.width + dimensions.normalNode.width / 2;
      const y = dimensions.nodeVerticalOffset(i, array.length);
      return {
        ...acc,
        [current.id]: { x, y, ...childNodeDimensions, ...current }
      };
    },
    {}
  );

  const parentsOfChildren = children.reduce(
    (acc, current) => {
      const pOc: any = graphs[current.id]
        .findNodesByTag(nodeKind.parent)
        .map(n => ({
          ...n,
          yRelativePosition: sign(n.yPosition - current.yPosition)
        }));
      return [...acc, ...pOc];
    },
    []
  );
  const relPosCnt2 = relativeSlotsCounter();
  const parentsOfChildrenPositions = _.orderBy(
    _.uniqBy(parentsOfChildren, "id"),
    "order"
  ).reduce(
    (acc, current, i, array) => {
      const x = dimensions.parentOffset + dimensions.normalNode.width / 2;
      const y = dimensions.canva.height / 2 +
        current.yRelativePosition *
          (dimensions.canva.height / 2 +
            relPosCnt2(current.yRelativePosition) *
              dimensions.normalNode.height) +
        dimensions.normalNode.height / 2;
      return {
        ...acc,
        [current.id]: { x, y, ...parentNodeDimensions, ...current }
      };
    },
    {}
  );

  // When a node is child of a rendered parent, and parent of a rendered child, it should be laid out between them (x) in the hidden space
  const middleNodes = _.filter(
    parentsOfChildrenPositions,
    p => p.id in childrenOfParentsPositions
  );

  return {
    ...parentOfParentsPositions,
    ...childrenOfChildrenPositions,
    ...parentsOfChildrenPositions,
    ...childrenOfParentsPositions,
    ...middleNodes.reduce(
      (acc, current) => ({
        ...acc,
        [current.id]: { ...current, x: dimensions.canva.width / 2 }
      }),
      {}
    )
  };
};
