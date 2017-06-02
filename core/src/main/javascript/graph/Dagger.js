// @flow

import injectSheet from "react-jss";
import React from "react";
import reduce from "lodash/reduce";

import { Graph } from "./dagger/dataAPI/genericGraph";
import type { Node, Edge } from "./dagger/dataAPI/genericGraph";
import type { Tag } from "../datamodel";

import { transitionAction } from "./dagger/render/d3render";
import { buildDagger } from "./dagger/dagger";

type Props = {
  classes: any,
  currentNodeId: string,
  nodes: Node[],
  edges: Edge[],
  tags: Tag[]
};

class DaggerComponent extends React.Component {
  minimapContainer: any;
  navigatorContainer: any;
  svgNavigatorContainer: any;
  edgesContainer: any;
  nodesContainer: any;
  dagger: any;

  constructor(props: Props) {
    super(props);
  }

  shouldComponentUpdate(nextProps: Props) {
    return (
      nextProps.nodes.length !== this.props.nodes.length ||
      nextProps.edges.length !== this.props.edges.length ||
      nextProps.tags.length !== this.props.tags.length
    );
  }

  render() {
    const { classes } = this.props;
    return (
      <div className={classes.main}>
        <div
          className={classes.navigatorContainer}
          ref={el => (this.navigatorContainer = el)}
        >
          <svg
            width="100%"
            height="100%"
            ref={el => (this.svgNavigatorContainer = el)}
          >
            <defs>
              <filter id="blur" x="-20%" y="-20%" width="200%" height="200%">
                <feOffset result="offOut" in="SourceGraphic" />
                <feColorMatrix
                  result="matrixOut"
                  in="offOut"
                  type="matrix"
                  values="0.7 0 0 0 0 0 0.7 0 0 0 0 0 0.7 0 0 0 0 0 1 0"
                />
                <feGaussianBlur
                  result="blurOut"
                  in="matrixOut"
                  stdDeviation="3"
                />
                <feBlend in="SourceGraphic" in2="blurOut" mode="normal" />
              </filter>
            </defs>
            <g
              id="allNodesContainer"
              ref={element => (this.nodesContainer = element)}
            />
            <g
              id="allEdgesContainer"
              ref={element => (this.edgesContainer = element)}
            />
          </svg>
        </div>
        <div className={classes.nodeDescription} />
        <div
          className={classes.minimapContainer}
          ref={element => (this.minimapContainer = element)}
        />
      </div>
    );
  }

  componentDidMount() {
    const { nodes, edges, tags } = this.props;
    const overallGraph: Graph = new Graph(nodes, edges);
    const width = this.navigatorContainer.clientWidth;
    const height = this.navigatorContainer.clientHeight;
    this.svgNavigatorContainer.setAttribute("width", width);
    this.svgNavigatorContainer.setAttribute("height", height);
    this.dagger = buildDagger(overallGraph, {
      width,
      height,
      nodesContainer: this.nodesContainer,
      edgesContainer: this.edgesContainer,
      tags: reduce(
        tags,
        (acc, current) => ({ ...acc, [current.name]: current.color }),
        {}
      ),
      minimap: {
        container: this.minimapContainer,
        setup: minimap => {
          minimap.nodes().on("mouseover", event => {
            const target = event.cyTarget;
          });
          minimap.nodes().on("mouseout", event => {
            const target = event.cyTarget;
          });
        }
      }
    });

    this.dagger.initRender(transitionAction);
  }
}

const styles = {
  main: {
    outline: "none",
    border: "0px",
    display: "flex",
    flexDirection: "column",
    flex: 1,
    alignItems: "stretch"
  },
  nodeDescription: {
    backgroundColor: "#FFF",
    flex: 1,
    boxShadow: "0px 1px 5px 0px #BECBD6",
    margin: "0 5em"
  },
  minimapContainer: {
    flex: 1,
    marginTop: "2.5em",
    margin: "5em",
    padding: "2.5em",
    backgroundColor: "#FFF",
    boxShadow: "0px 1px 5px 0px #BECBD6"
  },
  navigatorContainer: {
    flex: 4
  }
};

export default injectSheet(styles)(DaggerComponent);
