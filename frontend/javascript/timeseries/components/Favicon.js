// @flow

import React from "react";
import roundedRectangle from "canvas-rounded-rectangle";
import type { Statistics } from "../datamodel";

type Props = {
  statistics: Statistics
};

const W = 64;

class Favicon extends React.Component<Props> {
  updateIcon() {
    let { statistics } = this.props;
    let icon = document.createElement("canvas");
    icon.width = icon.height = W;
    let context = icon.getContext("2d");
    let text = "・";

    if (context) {
      if (statistics.error) {
        context.fillStyle = "#f44336";
        text = "✕";
      } else if (statistics.failing) {
        context.fillStyle = "#E91E63";
        text = statistics.failing.toString();
      } else if (statistics.running) {
        context.fillStyle = "#00BCD4";
        text = statistics.running.toString();
      } else if (statistics.waiting) {
        context.fillStyle = "#ff9800";
        text = statistics.waiting.toString();
      } else {
        context.fillStyle = "#4c515f";
      }
      context.fill(
        roundedRectangle({
          top: 0,
          left: 0,
          width: W,
          height: W,
          borderRadius: W * 0.15
        })
      );

      let fontSize = W * 0.6;
      let textWidth = 0;
      while (!textWidth && fontSize) {
        context.font = `${fontSize}px Arial`;
        let width = context.measureText(text).width;
        if (width < W * 0.9) {
          textWidth = width;
        } else {
          fontSize--;
        }
      }
      context.fillStyle = "#ffffff";
      context.fillText(
        text,
        (W - textWidth) / 2,
        (W - fontSize) / 2 + fontSize * 0.9
      );

      let link = document.getElementsByTagName("link")[0];
      link.type = "image/x-icon";
      link.rel = "shortcut icon";
      link.href = icon.toDataURL("image/x-icon");
    }
  }

  componentDidMount() {
    this.updateIcon();
  }

  componentDidUpdate() {
    this.updateIcon();
  }

  render() {
    return null;
  }
}

export default Favicon;
