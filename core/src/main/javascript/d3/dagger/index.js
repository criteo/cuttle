import data from "./data/enginejoins";
import * as d3 from "d3";
import { Graph, Edge, Node } from "./dataAPI/genericGraph";

import { transitionAction, initContainers } from "./render/d3render";
import { buildDagger } from "./dagger";

const overallGraph: Graph = Graph.buildGraphFromAPIData(data);
const dagger = buildDagger(overallGraph, {
  container: "#navigator-container svg",
  minimap: {
    container: "#minimap-container",
    setup: minimap => {
      minimap.nodes().on("mouseover", event => {
        const target = event.cyTarget;
        d3
          .select("#minimap-hover-node")
          .style("opacity", 0)
          .text(target.id())
          .style("opacity", 1);
      });
      minimap.nodes().on("mouseout", event => {
        const target = event.cyTarget;
        d3.select("#minimap-hover-node").style("opacity", 0);
      });
    }
  }
});

const initPromise = dagger.initRender(transitionAction);
