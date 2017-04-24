// @flow

import injectSheet from "react-jss";
import React from "react";

import { Graph } from "../../d3/dagger/dataAPI/genericGraph";
import type { Node, Edge } from "../../d3/dagger/dataAPI/genericGraph";
import type { Tag } from "../../datamodel/workflow";

import { transitionAction } from "../../d3/dagger/render/d3render";
import { buildDagger } from "../../d3/dagger/dagger";

import * as d3 from "d3";

type Props = {
  width?: number,
  height?: number,
  currentNodeId: string,
  nodes: Node[],
  edges: Edge[],
  tags: Tag[]
};

class DaggerComponent extends React.Component {
  minimapContainer: any;
  edgesContainer: any;
  nodesContainer: any;

  constructor(props: Props) {
    super(props);
  }

  shouldComponentUpdate(nextProps: Props) {
    return nextProps.nodes.length !== this.props.nodes.length ||
      nextProps.edges.length !== this.props.edges.length ||
      nextProps.tags.length !== this.props.tags.length;
  }

  render() {
    return (
      <div id="navigator-container">
        <svg width="100%" height="850px">
          <defs>
            <filter id="blur" x="-20%" y="-20%" width="200%" height="200%">
              <feOffset result="offOut" in="SourceGraphic" />
              <feColorMatrix
                result="matrixOut"
                in="offOut"
                type="matrix"
                values="0.7 0 0 0 0 0 0.7 0 0 0 0 0 0.7 0 0 0 0 0 1 0"
              />
              <feGaussianBlur result="blurOut" in="matrixOut" stdDeviation="3" />
              <feBlend in="SourceGraphic" in2="blurOut" mode="normal" />
            </filter>
          </defs>
          <g
            id="allNodesContainer"
            ref={element => this.nodesContainer = element}
          />
          <g
            id="allEdgesContainer"
            ref={element => this.edgesContainer = element}
          />
        </svg>
        <div
          id="minimap-container"
          ref={element => this.minimapContainer = element}
        />
      </div>);
  }

  componentDidMount() {
    const { nodes, edges, tags } = this.props;
    const overallGraph: Graph = new Graph(nodes, edges);
    const dagger = buildDagger(overallGraph, {
      nodesContainer: this.nodesContainer,
      edgesContainer: this.edgesContainer,
      minimap: {
        container: this.minimapContainer,
        setup: minimap => {
          minimap.nodes().on("mouseover", event => {
            const target = event.cyTarget;
            //d3.select("#minimap-hover-node").style("opacity", 0).text(target.id()).style("opacity", 1);
          });
          minimap.nodes().on("mouseout", event => {
            const target = event.cyTarget;
            //d3.select("#minimap-hover-node").style("opacity", 0);
          });
        }
      }
    });

    dagger.initRender(transitionAction);
  }
}

const styles = {
  main: {
    outline: "none",
    border: "0px"
  }
};

export default injectSheet(styles)(DaggerComponent);
