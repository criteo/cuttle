// @flow

import injectSheet from "react-jss";
import React from "react";
import ReactDOM from "react-dom";
import reduce from "lodash/reduce";
import forEach from "lodash/forEach";

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

const updateDaggerDimensions = (dagger: any, width: number, height: number) =>
  dagger.updateDimensions(width, height);

const cleanDOMContainer = domNode => {
  domNode.childNodes.forEach(child => domNode.removeChild(child));
};

class DaggerComponent extends React.Component {
  minimapContainer: any;
  navigatorContainer: any;
  svgNavigatorContainer: any;
  edgesContainer: any;
  nodesContainer: any;

  dagger: any;
  timeMachine: any;

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
        <div
          className={classes.minimapContainer}
          ref={element => (this.minimapContainer = element)}
        />
      </div>
    );
  }

  componentDidMount() {
    const { nodes, edges, tags, startNodeId } = this.props;
    const overallGraph: Graph = new Graph(nodes, edges);
    const width = this.navigatorContainer.clientWidth;
    const height = this.navigatorContainer.clientHeight;
    this.svgNavigatorContainer.setAttribute("width", width);
    this.svgNavigatorContainer.setAttribute("height", height);
    this.dagger = buildDagger(overallGraph, {
      width,
      height,
      startNodeId,
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

    this.timeMachine = this.dagger.initRender(transitionAction);

    const resizeDagger = () => {
      // Clean dom nodes holders
      cleanDOMContainer(this.nodesContainer);
      cleanDOMContainer(this.edgesContainer);
      cleanDOMContainer(this.minimapContainer);
      // Resize svg container
      const width = this.navigatorContainer.clientWidth;
      const height = this.navigatorContainer.clientHeight;
      this.svgNavigatorContainer.setAttribute("width", width);
      this.svgNavigatorContainer.setAttribute("height", height);
      // Update layouts and rerender
      this.dagger = updateDaggerDimensions(this.dagger, width, height);
      this.timeMachine = this.dagger.initRender(transitionAction);
    };

    let doResize;
    window.onresize = () => {
      clearTimeout(doResize);
      doResize = setTimeout(resizeDagger, 500);
    };
  }
}

const styles = {
  main: {
    outline: "none",
    border: "0px",
    display: "flex",
    flexDirection: "column",
    flex: 2,
    alignItems: "stretch",
    overflow: "hidden"
  },
  minimapContainer: {
    flex: 1,
    margin: "1em",
    backgroundColor: "#F5F8FA",
    boxShadow: "0px 1px 5px 0px #BECBD6",
    width: "50%",
    minHeight: "300px",
    alignSelf: "flex-end"
  },
  navigatorContainer: {
    minWidth: "800px",
    flex: 7,
    overflow: "hidden"
  }
};

export default injectSheet(styles)(DaggerComponent);
