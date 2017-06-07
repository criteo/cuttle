// @flow

import injectSheet from "react-jss";
import React from "react";
import reduce from "lodash/reduce";

import { Graph } from "./dagger/dataAPI/genericGraph";
import type { Node, Edge } from "./dagger/dataAPI/genericGraph";
import type { Tag } from "../datamodel";

import { transitionAction } from "./dagger/render/d3render";
import { buildDagger } from "./dagger/dagger";

import { navigate } from "redux-url";
import { connect } from "react-redux";

type Props = {
  classes: any,
  nodes: Node[],
  edges: Edge[],
  tags: Tag[],
  startNodeId: string,
  navTo: () => void
};

const updateDaggerDimensions = (dagger: any, width: number, height: number, startNodeId: string) =>
  dagger.updateDimensions(width, height, startNodeId);

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

  shouldComponentUpdate(nextProps: Props) {
    return (
      nextProps.nodes.length !== this.props.nodes.length ||
      nextProps.edges.length !== this.props.edges.length ||
      nextProps.tags.length !== this.props.tags.length
    );
  }

  componentWillReceiveProps(nextProps: Props) {
    const transitionAction = this.dagger.transitionAction(this.nodesContainer, this.edgesContainer);
    if (this.timeMachine)
      this.timeMachine
      .next(nextProps.startNodeId, transitionAction)
      .then(({ timeMachine }) =>
        this.timeMachine = timeMachine);
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
        <div className={classes.minimapContainer}>
          <div
            className={classes.minimapInnerContainer}
            ref={element => (this.minimapContainer = element)}
          />
          <div
            className={classes.minimapHover}
            ref={element => this.minimapHover = element}
          />
        </div>
      </div>
    );
  }

  componentDidMount() {
    const { nodes, edges, tags, startNodeId, navTo } = this.props;
    const overallGraph: Graph = new Graph(nodes, edges);
    const width = this.navigatorContainer.clientWidth;
    const height = this.navigatorContainer.clientHeight;
    this.svgNavigatorContainer.setAttribute("width", width);
    this.svgNavigatorContainer.setAttribute("height", height);
    this.dagger = buildDagger(overallGraph, {
      width,
      height,
      startNodeId,
      onClickNode: id => navTo("/workflow/" + id),
      nodesContainer: this.nodesContainer,
      edgesContainer: this.edgesContainer,
      tags: reduce(
        tags,
        (acc, current) => ({ ...acc, [current.name]: current.color }),
        {}
      ),
      minimap: {
        container: this.minimapContainer,
        onClickNode: id => navTo("/workflow/" + id),
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

    this.dagger
      .initRender(transitionAction)
      .then(({ timeMachine }) => (this.timeMachine = timeMachine));

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
      this.dagger = updateDaggerDimensions(this.dagger, width, height, this.props.startNodeId);
      this.timeMachine = null;
      this.dagger
        .initRender(transitionAction)
        .then(({ timeMachine }) => (this.timeMachine = timeMachine));
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
    width: "100%",
    height: "100%"
  },
  minimapContainer: {
    backgroundColor: "rgba(255,255,255,.5)",
    boxShadow: "0px 1px 5px 0px #BECBD6",
    width: "300px",
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
    
  }
};

export default connect(
  () => ({}),
  dispatch => ({
    navTo: link => dispatch(navigate(link))
  })
)(injectSheet(styles)(DaggerComponent));
