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
    return (
      startVerticalOffset + order * dimensions.height + dimensions.height / 2
    );
  }

  static buildWidthDimensions(width = 1200) {
    if (width > 1000)
      return {
        reference: 1200,
        node: {
          small: {
            width: 240
          },
          normal: {
            width: 240
          }
        },
        grid: {
          childOffset: 840,
          parentOffset: 120
        }
      };
    else if (width > 800)
      return {
        reference: 1000,
        node: {
          small: {
            width: 240
          },
          normal: {
            width: 240
          }
        },
        grid: {
          childOffset: 660,
          parentOffset: 100
        }
      };
    else
      return {
        reference: 800,
        node: {
          small: {
            width: 200
          },
          normal: {
            width: 200
          }
        },
        grid: {
          childOffset: 575,
          parentOffset: 25
        }
      };
  }

  static buildHeightDimensions(height = 1200) {
    if (height > 1000)
      return {
        reference: 1200,
        node: {
          small: {
            height: 30,
            margin: 0.1
          },
          normal: {
            height: 100,
            margin: 0.5
          }
        },
        grid: {
          childOffset: 840,
          parentOffset: 120
        }
      };
    else if (height > 800)
      return {
        reference: 1000,
        node: {
          small: {
            height: 30,
            margin: 0.1
          },
          normal: {
            height: 100,
            margin: 0.5
          }
        },
        grid: {
          childOffset: 700,
          parentOffset: 100
        }
      };
    else if (height > 600)
      return {
        reference: 800,
        node: {
          small: {
            height: 30,
            margin: 0.1
          },
          normal: {
            height: 80,
            margin: 0.3
          }
        },
        grid: {
          childOffset: 450,
          parentOffset: 50
        }
      };
    else
      return {
        reference: 600,
        node: {
          small: {
            height: 30,
            margin: 0.1
          },
          normal: {
            height: 60,
            margin: 0.25
          }
        },
        grid: {
          childOffset: 450,
          parentOffset: 50
        }
      };
  }

  static buildDefaultDimensions({ width = 1200, height = 850 }) {
    // Here we define default dimensions
    // These default dimensions are used in a relative way
    // when parameters are provided (responsive behaviour)
    const wDim = GraphDimensions.buildWidthDimensions(width);
    const hDim = GraphDimensions.buildHeightDimensions(height);
    const widthFactor = width / wDim.reference;
    const heightFactor = height / hDim.reference;

    return new GraphDimensions({
      canva: {
        height,
        width
      },
      childOffset: wDim.grid.childOffset * widthFactor,
      parentOffset: wDim.grid.parentOffset * widthFactor,
      smallNode: {
        width: wDim.node.small.width * widthFactor,
        height: hDim.node.small.height * heightFactor,
        margin: hDim.node.small.margin
      },
      normalNode: {
        width: wDim.node.normal.width * widthFactor,
        height: hDim.node.normal.height * heightFactor,
        margin: hDim.node.normal.margin
      }
    });
  }
}
