import {nodeKind, edgeKind} from '../layout/symbolic/annotatedGraph';
import * as d3 from 'd3';
import forEach from "lodash/forEach";
import {interpolatePath} from 'd3-interpolate-path';

const transitionDuration = 500;
const transitionDelay = 0;
const transitionEase = d3.easeLinear;
const widthReference = 30;
const widthMax = 1.3 * widthReference;
const widthMin = 0.7 * widthReference;
const realWidths = {};

const dpath = (source, target, kind, x1, y1, x2, y2) => {
  let start, end;
  switch(kind) {
    case edgeKind.centerToChild:
      start = {x: source.x + realWidths[source.id]/2, y: y1};
      end = {x: target.x - target.width/2, y: y2};
      break;
    case edgeKind.parentToCenter:
      start = {x: source.x + source.width/2, y: y1};
      end = {x: target.x - realWidths[target.id]/2, y: y2};
      break;
    case edgeKind.missingParentToParent:
      start = source;
      end = {x: target.x - (target.id in realWidths ? realWidths[target.id]: target.width) + target.width/2, y: y2};
      break;
    case edgeKind.parentToMissingChild:
      start = {x: source.x + source.width/2, y: y1};
      end = target;
      break;
    case edgeKind.missingParentToChild:
      start = source;
      end = {x: target.x - target.width/2, y: y2};
      break;
    case edgeKind.childToMissingChild:
      start = {x: source.x - source.width/2 + (source.id in realWidths ? realWidths[source.id]: source.width), y: y1};
      end = target;
      break;
    default:
      return {
        path: "M" + x1.toFixed(2) + "," + y1.toFixed(2) + "L" + x2.toFixed(2) + "," + y2.toFixed(2),
        start: {x: x1, y: y1}, end: {x: x2, y: y2}
      };
  }
  return {
    path: "M" + start.x.toFixed(2) + "," + start.y.toFixed(2) +
      "L" + (start.x + 30).toFixed(2) + "," + start.y.toFixed(2) +
      "L" + (end.x - 30).toFixed(2) + "," + end.y.toFixed(2) +
      "L" + end.x.toFixed(2) + "," + end.y.toFixed(2),
    start, end
  };
};

const arrowHead = (cos, sin, x, y, width, height) =>
  "M" + (x-width).toFixed(2) + "," + (y-height/2).toFixed(2) +
  "L" + x.toFixed(2) + "," + y.toFixed(2) +
  "L" + (x-width).toFixed(2) + "," + (y+height/2).toFixed(2);

const adjustNodePosition = (kind, width, realWidth, height, x, y) => {
  if(kind === nodeKind.parent)
    return "translate(" + (x + width/2 - realWidth).toFixed(2) + "," + (y - height/2).toFixed(2) + ")";
  else if(kind === nodeKind.main)
    return "translate(" + (x - realWidth/2).toFixed(2) + "," + (y - height/2).toFixed(2) + ")";
  else
    return "translate(" + (x - width/2).toFixed(2) + "," + (y - height/2).toFixed(2) + ")";
}

export const transitionEdge = (edge, {x1, y1, x2, y2, kind, id, source, target}, nodePositions) => {
  const transition = edge
    .transition().delay(transitionDelay).duration(transitionDuration);

  const parentOrChild = kind === edgeKind.parentToCenter || kind === edgeKind.centerToChild;
  const lineOpacity = parentOrChild ? 1 : 0.2;
  const lineColor = "#909AB9";
  const path = transition.select("path.lineBody")
    .style("stroke", lineColor)
    .style("opacity", lineOpacity)

  const newPath = dpath(nodePositions[source], nodePositions[target], kind, x1, y1, x2, y2);
  const head = transition.select("path.arrowHead")
    .style("stroke", lineColor)
    .attr("d", arrowHead(0, 0, newPath.end.x, newPath.end.y, 10, 5))
    .style("opacity", lineOpacity)

  path.attrTween("d", (d, i, nodes) => {
    const prev = d3.select(nodes[i]).attr("d");
    return interpolatePath(prev, newPath.path)
  });
  return transition;
}

export const transitionNode = (node, {x, y, width, height, id, order, yPosition, kind}) => {
  const newWidth = realWidths[id];
  const transition = node
    .transition().delay(transitionDelay).duration(transitionDuration)
    .attr("transform", adjustNodePosition(kind, width, newWidth, height, x, y));
  transition.select("rect")
    .attr("width", newWidth.toFixed(2))
    .attr("height", height);
  transition.select("text")
    .attr("x", (newWidth/2).toFixed(2))
    .attr("y", height/2)

  return transition;
}

export const drawEdge = (domContainer, {x1, y1, x2, y2, kind, id, source, target}, nodePositions) => {
  const edge = domContainer
    .append("g").attr("id", id).attr("class", "oneEdge");

  const parentOrChild = kind === edgeKind.parentToCenter || kind === edgeKind.centerToChild ;
  const lineOpacity = parentOrChild ? 1 : 0.2;
  const lineColor = "#909AB9";

  const newPath = dpath(nodePositions[source], nodePositions[target], kind, x1, y1, x2, y2);
  const head = edge.append("path").classed("arrowHead", true)
    .attr("fill", "none")
    .style("stroke", lineColor)
    .style("stroke-width", 1)
    .style("opacity", lineOpacity)
    .attr("d", arrowHead(0, 0, newPath.end.x, newPath.end.y, 10, 5));

  const path = edge
    .append("path").classed("lineBody", true)
    .attr("fill", "none")
    .style("stroke", lineColor)
    .style("stroke-width", 1)
    .style("opacity", lineOpacity)
    .attr("d", newPath.path);

  return edge;
};

const computeNewWidth = (label: string, stringLengthReference: number, pixelWidthReference: number, widthMax: number, widthMin:number) => {
  const labelLength = Array.from(label).length;
  const overflowCharacters = Math.max(labelLength - widthMax, 0); 
  const maxUsableWidth = pixelWidthReference * Math.max(Math.min(labelLength, widthMax), widthMin)/widthReference;

  const labelToDisplay = overflowCharacters > 0
    ? label.substring(0, labelLength - overflowCharacters - 3) +  "..."
    : label;
  return [maxUsableWidth, labelToDisplay];
};

const tagBulletVerticalOffset = ({
  height
}) => {
  const bulletSize = height/5;
  const spaceBetweenBullets = bulletSize/2;
  const startOffset = spaceBetweenBullets;
  return tagIndex => startOffset + tagIndex * (bulletSize + spaceBetweenBullets);
};

export const drawNode = (domContainer, {x, y, width, height, id, order, tags, name, yPosition, kind}, allTags) => {
  const node = domContainer
    .append("g").attr("id", id).attr("class", "oneNode");

  const [newWidth, nameToDisplay] = computeNewWidth(name || id, widthReference, width, widthMax, widthMin);
  realWidths[id] = newWidth;
  
  node
    .append("rect")
    .attr("width", newWidth.toFixed(2))
    .attr("height", height)
    .style("fill", "#E1EFFA")
    .attr("rx", 4).attr("ry", 4)
    .style("stroke", "#909AB9")
    .style("stroke-width", 0.5)
    .attr("filter", "url(#blur)");

  node
    .append("text")
    .attr("x", (newWidth/2).toFixed(2))
    .attr("y", height/2)
    .style("fill", "black")
    .style("font-family", "Arial")
    .style("font-weight", "bold")
    .style("alignment-baseline", "middle")
    .style("pointer-events", "none")
    .style("text-anchor", "middle")
    .style("font-size", "14px")
    .text(nameToDisplay);

  

  const bulletOffsetComputer = tagBulletVerticalOffset({ height });
  forEach(tags, (name, i) => {
    node.append("rect")
      .attr("width", 10)
      .attr("height", 10)
      .attr("rx", 3).attr("ry", 3)
      .style("fill", allTags[name] || "#AAA")
      .attr("x", 6).attr("y", bulletOffsetComputer(i))
  }); 

  node.attr("transform", adjustNodePosition(kind, width, newWidth, height, x, y));

  return node;
};
