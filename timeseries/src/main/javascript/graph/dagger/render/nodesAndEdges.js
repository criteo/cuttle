import { nodeKind, edgeKind } from "../layout/symbolic/annotatedGraph";
import * as d3 from "d3";
import { textWrap } from "d3plus-text";
import { forEach, identity, reduce } from "lodash";
import { interpolatePath } from "d3-interpolate-path";

const transitionDuration = 500;
const transitionDelay = 0;

const computeNewWidth = (
  label: string,
  stringLengthReference: number,
  pixelWidthReference: number,
  widthMax: number,
  widthMin: number
) => {
  const labelLength = Array.from(label).length;
  const pixelsPerLetter = pixelWidthReference / stringLengthReference;
  const maxUsableWidth =
    pixelWidthReference +
    (Math.max(Math.min(labelLength + 2, widthMax), widthMin) - widthReference) *
      pixelsPerLetter +
    2 * pixelsPerLetter;
  return maxUsableWidth;
};

const computeNewLabel = (label: string, widthMax: number) => {
  const labelLength = label.length;
  const overflowCharacters = Math.max(labelLength - widthMax, 0);
  const labelToDisplay =
    overflowCharacters > 0
      ? label.substring(0, labelLength - overflowCharacters - 4) + "..."
      : label;
  return labelToDisplay;
};

const widthReference = 26;
const widthMax = 1 * widthReference;
const widthMin = 0.3 * widthReference;

let realWidths = {};

export const cleanRealWidths = keys => {
  forEach(keys, k => delete realWidths[k]);
};

const getRealWidth = (id, name, width) => {
  return (
    realWidths[id] ||
    (realWidths[id] = computeNewWidth(
      name || id,
      widthReference,
      width,
      widthMax,
      widthMin
    ))
  );
};

const floatPrecision = 2;
const truncate = number => number.toFixed(floatPrecision);

const dpath = (source, target, kind, x1, y1, x2, y2) => {
  let start, end;
  let path;
  switch (kind) {
    case edgeKind.centerToChild:
      start = {
        x: source.x + getRealWidth(source.id, source.name, source.width) / 2,
        y: y1
      };
      end = { x: target.x - target.width / 2, y: y2 };
      break;
    case edgeKind.parentToCenter:
      start = { x: source.x + source.width / 2, y: y1 };
      end = {
        x: target.x - getRealWidth(target.id, target.name, target.width) / 2,
        y: y2
      };
      break;
    case edgeKind.missingParentToParent:
      start = source;
      end = {
        x:
          target.x -
          getRealWidth(target.id, target.name, target.width) +
          target.width / 2,
        y: y2
      };
      break;
    case edgeKind.parentToMissingChild:
      start = { x: source.x + source.width / 2, y: y1 };
      end = target;
      break;
    case edgeKind.missingParentToChild:
      start = source;
      end = { x: target.x - target.width / 2, y: y2 };
      break;
    case edgeKind.childToMissingChild:
      start = {
        x:
          source.x -
          source.width / 2 +
          getRealWidth(source.id, source.name, source.width),
        y: y1
      };
      end = target;
      break;
    default:
      return {
        path: ((path = d3.path()),
        path.moveTo(truncate(x1), truncate(y1)),
        path.lineTo(truncate(x2), truncate(y2)),
        path.toString()),
        start: { x: x1, y: y1 },
        end: { x: x2, y: y2 }
      };
  }
  return {
    path: ((path = d3.path()),
    path.moveTo(truncate(start.x), truncate(start.y)),
    path.lineTo(truncate(start.x + 30), truncate(start.y)),
    path.lineTo(truncate(end.x - 30), truncate(end.y)),
    path.lineTo(truncate(end.x), truncate(end.y)),
    path.toString()),
    start,
    end
  };
};

const arrowHead = (cos, sin, x, y, width, height, p = null) => (
  (p = d3.path()),
  p.moveTo(truncate(x - width), truncate(y - height / 2)),
  p.lineTo(truncate(x), truncate(y)),
  p.lineTo(truncate(x - width), truncate(y + height / 2)),
  p.toString()
);

const adjustNodePosition = (kind, width, realWidth, height, x, y) => {
  if (kind === nodeKind.parent)
    // parents are aligned on the right border
    return (
      "translate(" +
      truncate(x + width / 2 - realWidth) +
      "," +
      truncate(y - height / 2) +
      ")"
    );
  else if (kind === nodeKind.main)
    return (
      "translate(" +
      truncate(x - realWidth / 2) +
      "," +
      truncate(y - height / 2) +
      ")"
    );
  // children are aligned on the left border
  else
    return (
      "translate(" +
      truncate(x - width / 2) +
      "," +
      truncate(y - height / 2) +
      ")"
    );
};

export const transitionEdge = (
  edge,
  { x1, y1, x2, y2, kind, id, source, target },
  nodePositions
) => {
  const transition = edge
    .transition()
    .delay(transitionDelay)
    .duration(transitionDuration);

  const parentOrChild =
    kind === edgeKind.parentToCenter || kind === edgeKind.centerToChild;
  const lineOpacity = parentOrChild ? 1 : 0.2;
  const lineColor = "#909AB9";
  const path = transition
    .select("path.lineBody")
    .style("stroke", lineColor)
    .style("opacity", lineOpacity);

  const newPath = dpath(
    nodePositions[source],
    nodePositions[target],
    kind,
    x1,
    y1,
    x2,
    y2
  );
  const head = transition
    .select("path.arrowHead")
    .style("stroke", lineColor)
    .attr("d", arrowHead(0, 0, newPath.end.x, newPath.end.y, 10, 5))
    .style("opacity", lineOpacity);

  path.attrTween("d", (d, i, nodes) => {
    const prev = d3.select(nodes[i]).attr("d");
    return interpolatePath(prev, newPath.path);
  });
  return transition;
};

export const transitionNode = (
  node,
  { x, y, width, height, id, name, order, yPosition, kind }
) => {
  const newWidth = getRealWidth(id, name, width);
  const transition = node
    .transition()
    .delay(transitionDelay)
    .duration(transitionDuration)
    .attr("transform", adjustNodePosition(kind, width, newWidth, height, x, y));
  transition
    .select("rect")
    .attr("width", truncate(newWidth))
    .attr("height", truncate(height));
  transition
    .select("text")
    .attr("x", truncate(newWidth / 2))
    .attr("y", truncate(height / 2));

  return transition;
};

export const drawEdge = (
  domContainer,
  { x1, y1, x2, y2, kind, id, source, target },
  nodePositions
) => {
  const edge = domContainer
    .append("g")
    .attr("id", id)
    .attr("class", "oneEdge");

  const parentOrChild =
    kind === edgeKind.parentToCenter || kind === edgeKind.centerToChild;
  const lineOpacity = parentOrChild ? 1 : 0.2;
  const lineColor = "#909AB9";

  const newPath = dpath(
    nodePositions[source],
    nodePositions[target],
    kind,
    x1,
    y1,
    x2,
    y2
  );
  const head = edge
    .append("path")
    .classed("arrowHead", true)
    .attr("fill", "none")
    .style("stroke", lineColor)
    .style("stroke-width", 1)
    .style("opacity", lineOpacity)
    .attr("d", arrowHead(0, 0, newPath.end.x, newPath.end.y, 10, 5));

  const path = edge
    .append("path")
    .classed("lineBody", true)
    .attr("fill", "none")
    .style("stroke", lineColor)
    .style("stroke-width", 1)
    .style("opacity", lineOpacity)
    .attr("d", newPath.path);

  return edge;
};

const tagBulletVerticalOffset = ({ height }) => {
  const bulletSize = height / 5;
  const spaceBetweenBullets = bulletSize / 2;
  const startOffset = spaceBetweenBullets;
  return tagIndex =>
    startOffset + tagIndex * (bulletSize + spaceBetweenBullets);
};

// join words by whitespace, unless it's delimited by -_
const joinWords = (words: Array<string>): string =>
  reduce(words, (acc, word) =>
    /[-_]$/.test(acc) ? acc + word : acc + " " + word
  );

export const drawNode = (
  domContainer,
  { x, y, width, height, id, name, kind },
  tags
) => {
  const fontSize = 12;
  const node = domContainer
    .append("g")
    .attr("id", id)
    .attr("class", "oneNode");
  const newWidth = getRealWidth(id, name, width);
  const color = (tags && tags[id]) || "#E1EFFA";

  node
    .append("rect")
    .attr("width", truncate(newWidth))
    .attr("height", truncate(height))
    .style("fill", color)
    .attr("rx", 4)
    .attr("ry", 4)
    .style("stroke", "#909AB9")
    .style("stroke-width", 0.5)
    .style("box-shadow", "10px 10px 5px #888888;")
    .attr("filter", "url(#blur)");

  const wrapped = textWrap()
    .fontSize(fontSize)
    .width(newWidth * 0.8)(name);
  const [first, ...rest] = wrapped.words;
  const textPadding = (height - 2 * fontSize) / 4;
  if (rest.length === 0 || textPadding < 3) {
    // single line
    node
      .append("text")
      .style("text-anchor", "middle")
      .text(computeNewLabel(first, newWidth / (fontSize / 1.5)));
  } else {
    // if there's enough space, display 2 lines
    node
      .append("text")
      .selectAll("tspan")
      .data(
        [first, joinWords(rest)].map(line =>
          computeNewLabel(line, newWidth / (fontSize / 1.5))
        )
      )
      .enter()
      .append("tspan")
      .attr("x", fontSize)
      .attr("dy", (d, i) => -textPadding + i * (2 * textPadding + fontSize))
      .text(identity);
  }

  // adjust styles
  node
    .selectAll("text")
    .attr("x", truncate(newWidth / 2))
    .attr("y", truncate(height / 2))
    .style("font-size", `${fontSize}px`)
    .style("fill", "black")
    .style("font-family", "Fira Mono")
    .style("font-weight", "500")
    .style("alignment-baseline", "middle")
    .append("title")
    .text(name);

  node.attr(
    "transform",
    adjustNodePosition(kind, width, newWidth, height, x, y)
  );

  return node;
};
