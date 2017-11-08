// @flow

import injectSheet from "react-jss";
import React from "react";
import isEqual from "lodash/isEqual";
import map from "lodash/map";
import constant from "lodash/constant";

import { Graph } from "./dagger/dataAPI/genericGraph";
import type { Node, Edge } from "./dagger/dataAPI/genericGraph";
import type { Tags } from "../datamodel";

import { transitionAction } from "./dagger/render/d3render";
import { cleanRealWidths } from "./dagger/render/nodesAndEdges";
import { buildDagger } from "./dagger/dagger";

type Props = {
  classes: any,
  nodes: Node[],
  edges: Edge[],
  tags: Tags,
  startNodeId: string,
  onClickNode: string => void
};

const cleanDOMContainer = domNode => {
  while (domNode.firstChild)
    domNode.removeChild(domNode.firstChild);
};

class DaggerComponent extends React.Component {
  minimapContainer: any;
  minimapHover: any;
  navigatorContainer: any;
  svgNavigatorContainer: any;
  edgesContainer: any;
  nodesContainer: any;

  dagger: any;
  timeMachine: any;

  constructor(props: Props) {
    super(props);
  }

  shouldComponentUpdate = constant(false);

  componentWillReceiveProps(nextProps: Props) {
    const { nodes, edges, tags, startNodeId, onClickNode } = nextProps;
    if (
      nextProps.nodes.length !== this.props.nodes.length ||
      !isEqual(nextProps.tags, this.props.tags)
    )
      this.buildGraph(nodes, edges, tags, startNodeId, onClickNode);

    const transitionAction = this.dagger.transitionAction(
      this.nodesContainer,
      this.edgesContainer
    );
    if (this.timeMachine && nextProps.startNodeId !== this.props.startNodeId)
      this.timeMachine
        .next(nextProps.startNodeId, transitionAction)
        .then(({ timeMachine }) => (this.timeMachine = timeMachine));
  }

  renderFilters() {
    return (
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
            {this.renderFilters()}
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
        <div className={classes.minimapContainer}>
          <div
            className={classes.minimapInnerContainer}
            ref={element => (this.minimapContainer = element)}
          />
          <div
            className={classes.minimapHover}
            ref={element => (this.minimapHover = element)}
          />
        </div>
      </div>
    );
  }

  componentDidMount() {
    const { nodes, edges, tags, startNodeId, onClickNode } = this.props;

    this.buildGraph(nodes, edges, tags, startNodeId, onClickNode);

    this.dagger
      .initRender(transitionAction)
      .then(({ timeMachine }) => (this.timeMachine = timeMachine));

    let doResize;
    window.onresize = () => {
      clearTimeout(doResize);
      doResize = setTimeout(
        () => this.buildGraph(nodes, edges, tags, startNodeId, onClickNode),
        500
      );
    };
  }

  buildGraph(nodes, edges, tags, startNodeId, onClickNode) {
    // Clean dom nodes holders
    cleanRealWidths(map(nodes, "id"));
    cleanDOMContainer(this.nodesContainer);
    cleanDOMContainer(this.edgesContainer);
    cleanDOMContainer(this.minimapContainer);
    // Resize svg container
    const width = this.navigatorContainer.clientWidth;
    const height = this.navigatorContainer.clientHeight;
    this.svgNavigatorContainer.setAttribute("width", width);
    this.svgNavigatorContainer.setAttribute("height", height);
    // Update layouts and rerender
    this.dagger = buildDagger(new Graph(nodes, edges), {
      width,
      height,
      startNodeId,
      onClickNode,
      nodesContainer: this.nodesContainer,
      edgesContainer: this.edgesContainer,
      tags: tags,
      minimap: {
        container: this.minimapContainer,
        onClickNode,
        setup: minimap => {
          minimap.nodes().on("mouseover", event => {
            const target = event.cyTarget;
            this.minimapHover.innerText = target.id();
          });
          minimap.nodes().on("mouseout", () => {
            this.minimapHover.innerText = "";
          });
        }
      }
    });

    this.timeMachine = null;
    this.dagger
      .initRender(transitionAction)
      .then(({ timeMachine }) => (this.timeMachine = timeMachine));
  }
}

const styles = {
  main: {
    outline: "none",
    border: "0px",
    width: "100%",
    height: "100%",
    position: "relative"
  },
  minimapContainer: {
    backgroundColor: "rgba(255,255,255,.5)",
    boxShadow: "0px 1px 5px 0px #BECBD6",
    width: "300px",
    marginLeft: "-150px",
    position: "absolute",
    bottom: "1em",
    borderRadius: "0.2em",
    left: "50%"
  },
  minimapHover: {
    position: "absolute",
    bottom: "0.5em",
    width: "100%",
    fontFamily: "Arial",
    textAlign: "center",
    fontSize: "0.8em",
    fontWeight: "bold",
    color: "rgba(92,100,119, 0.75)"
  },
  minimapInnerContainer: {
    height: "150px",
    width: "100%"
  },
  navigatorContainer: {
    overflow: "hidden",
    width: "100%",
    height: "100%"
  },
  jobSelector: {
    position: "absolute",
    top: "2em",
    left: "50%",
    marginLeft: "-300px",
    width: "600px",
    "& .Select-control": {
      borderRadius: "1em",
      height: "1em",
      backgroundColor: "#F5F8FA",
      "& .Select-value": {
        color: "#A9B8C3",
        fontSize: "0.9em"
      },
      "& .Select-menu ! important": {
        margin: "0 1em",
        width: "calc(600px - 2em)"
      },
      "& .Select-menu-outer !important": {
        margin: "0 1em",
        width: "calc(600px - 2em)"
      },
      "& .Select-option !important": {
        fontSize: "0.9em"
      },
      "& .Select-arrow-zone": {
        display: "none"
      }
    }
  }
};

export default injectSheet(styles)(DaggerComponent);
