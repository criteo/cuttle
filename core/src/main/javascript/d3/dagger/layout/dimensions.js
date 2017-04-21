// @flow

export type Box = {
  width: number,
  height: number,
  margin: number
};

export type Path = {
  lineWidth: number
}

export class GraphDimensions {
  canva: $Shape<Box>;
  smallNode: Box;
  normalNode: Box;
  edge: Path;
  parentOffset: number;
  childOffset: number;

  nodeDimensions(nbElements: number) {
    return nbElements > 8 ? this.smallNode : this.normalNode;
  }

  nodeSize(nbElements: number) {
    const nodeDimensions = this.nodeDimensions(nbElements);
    return {
      width: nodeDimensions.width,
      height: nodeDimensions.height * (1 - nodeDimensions.margin),
    };
  }

  nodeVerticalOffset(order: number, nbElements: number) {
    const dimensions = this.nodeDimensions(nbElements);
    const verticalBand = dimensions.height * nbElements;
    const startVerticalOffset = (this.canva.height - verticalBand)/2;
    return startVerticalOffset + order * dimensions.height + dimensions.height/2;
  }

  static buildDefaultDimensions() {
    const dim = new GraphDimensions();
    dim.canva = {
      height: 850,
      width: 1200
    };
    dim.childOffset = 840;
    dim.parentOffset = 120;
    dim.smallNode = {
      width: 240,
      height: 30,
      margin: 0.1
    };
    dim.normalNode = {
      width: 240,
      height: 100,
      margin: 0.5
    };
    return dim;
  }
};
