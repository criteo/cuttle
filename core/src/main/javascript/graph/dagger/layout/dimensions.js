// @flow

export type Box = {
  width: number,
  height: number,
  margin: number
};

export type Path = {
  lineWidth: number
};

export class GraphDimensions {
  edge: Path;
  canva: $Shape<Box>;
  smallNode: Box;
  normalNode: Box;
  parentOffset: number;
  childOffset: number;

  constructor(dim: any) {
    this.canva = dim.canva;
    this.smallNode = dim.smallNode;
    this.normalNode = dim.normalNode;
    this.parentOffset = dim.parentOffset;
    this.childOffset = dim.childOffset;
  }

  nodeDimensions(nbElements: number) {
    return nbElements > 8 ? this.smallNode : this.normalNode;
  }

  nodeSize(nbElements: number) {
    const nodeDimensions = this.nodeDimensions(nbElements);
    return {
      width: nodeDimensions.width,
      height: nodeDimensions.height * (1 - nodeDimensions.margin)
    };
  }

  nodeVerticalOffset(order: number, nbElements: number) {
    const dimensions = this.nodeDimensions(nbElements);
    const verticalBand = dimensions.height * nbElements;
    const startVerticalOffset = (this.canva.height - verticalBand) / 2;
    return startVerticalOffset +
      order * dimensions.height +
      dimensions.height / 2;
  }

  static buildDefaultDimensions({ width = 1200, height = 850 }) {
    // Here we define default dimensions
    // These default dimensions are used in a relative way
    // when parameters are provided (responsive behaviour)

    const referenceWidth = 1200;
    const referenceHeight = 850;
    const widthFactor = width / referenceWidth;
    const heightFactor = height / referenceHeight;

    const grid = {
      childOffset: 840,
      parentOffset: 120
    };

    const node = {
      small: {
        width: 240,
        height: 30,
        margin: 0.1
      },
      normal: {
        width: 240,
        height: 100,
        margin: 0.5
      }
    };

    return new GraphDimensions({
      canva: {
        height,
        width
      },
      childOffset: grid.childOffset * widthFactor,
      parentOffset: grid.parentOffset * heightFactor,
      smallNode: {
        width: node.small.width * widthFactor,
        height: node.small.height * heightFactor,
        margin: node.small.margin
      },
      normalNode: {
        width: node.normal.width * widthFactor,
        height: node.normal.height * heightFactor,
        margin: node.normal.margin
      }
    });
  }
}
