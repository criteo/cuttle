// @flow

export type Box = {
  width: number,
  height: number,
  margin: number
};

export type Path = {
  lineWidth: number
};

export type Grid = {
  parentOffset: number,
  childOffset: number,
  top: number,
  bottom: number,
  mainX: number,
  mainY: number
};

export class GraphDimensions {
  edge: Path;
  canva: $Shape<Box>;
  smallNode: Box;
  normalNode: Box;
  grid: Grid;

  constructor(dim: any) {
    this.canva = dim.canva;
    this.smallNode = dim.smallNode;
    this.normalNode = dim.normalNode;
    this.grid = dim.grid;
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
    const startVerticalOffset =
      this.grid.top + (this.grid.bottom - this.grid.top - verticalBand) / 2;
    return (
      startVerticalOffset + order * dimensions.height + dimensions.height / 2
    );
  }

  static buildWidthDimensions(width: number = 1200) {
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
          mainX: 600,
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
          mainX: 500,
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
          mainX: 400,
          parentOffset: 25
        }
      };
  }

  static buildHeightDimensions(height: number = 1200) {
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
          top: 0,
          bottom: 1200,
          mainY: 600
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
          top: 100,
          bottom: 700,
          mainY: 400
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
          top: 100,
          bottom: 600,
          mainY: 350
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
          top: 100,
          bottom: 400,
          mainY: 250
        }
      };
  }

  static buildDefaultDimensions({ width = 1200, height = 850 }: { width: number, height: number }) {
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
      grid: {
        childOffset: wDim.grid.childOffset * widthFactor,
        parentOffset: wDim.grid.parentOffset * widthFactor,
        mainX: wDim.grid.mainX * widthFactor,
        mainY: hDim.grid.mainY * heightFactor,
        top: hDim.grid.top * heightFactor,
        bottom: hDim.grid.bottom * heightFactor
      },
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
