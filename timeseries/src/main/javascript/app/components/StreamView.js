// @flow

import classNames from "classnames";
import * as React from "react";
import { highlightURLs } from "../utils/URLHighlighter";
import injectSheet from "react-jss";
import FullscreenIcon from "react-icons/lib/md/fullscreen";
import ExitFullscreenIcon from "react-icons/lib/md/fullscreen-exit";
import AutoScrollIcon from "react-icons/lib/md/arrow-downward";

type Props = {
  classes: Object,
  streams: Array<{
    timestamp: string,
    level: string,
    message: string
  }>,
  placeholder: ?React.ChildrenArray<React.Node>
};

type State = {
  fullScreen: boolean,
  autoScroll: boolean
};

class StreamView extends React.Component<Props, State> {
  state = {
    fullScreen: false,
    autoScroll: true
  };
  scroller: ?{ scrollTop: number, scrollHeight: number, offsetHeight: number };

  detectManualScroll() {
    const manualScroll =
      this.scroller &&
      this.scroller.scrollHeight - this.scroller.offsetHeight !==
        this.scroller.scrollTop;

    if (manualScroll)
      this.setState({
        autoScroll: false
      });
  }

  onClickFullScreen(fullScreen: boolean) {
    this.setState({ fullScreen });
  }

  onClickAutoScroll(autoScroll: boolean) {
    this.setState({ autoScroll });
  }

  componentDidUpdate() {
    if (this.scroller && this.state.autoScroll) {
      this.scroller.scrollTop = this.scroller.scrollHeight;
    }
  }

  render() {
    const props = this.props;
    const className = classNames(props.classes.streams, {
      [props.classes.fullScreen]: this.state.fullScreen
    });
    return (
      <div className={className}>
        <ul
          ref={r => (this.scroller = r)}
          onScroll={this.detectManualScroll.bind(this)}
        >
          {props.streams.map(({ timestamp, level, message }, i) => {
            return (
              <li key={i}>
                <span>{timestamp}</span>
                <div className={props.classes[level]}>
                  {highlightURLs(message)}
                </div>
              </li>
            );
          })}
          {props.placeholder}
        </ul>
        {this.state.fullScreen ? (
          <ExitFullscreenIcon
            onClick={this.onClickFullScreen.bind(this, false)}
            className={props.classes.fullScreenButton}
          />
        ) : (
          <FullscreenIcon
            onClick={this.onClickFullScreen.bind(this, true)}
            className={props.classes.fullScreenButton}
          />
        )}
        <AutoScrollIcon
          onClick={this.onClickAutoScroll.bind(this, !this.state.autoScroll)}
          className={classNames(props.classes.autoScrollButton, {
            [props.classes.activeAutoScroll]: this.state.autoScroll
          })}
        />
      </div>
    );
  }
}

const styles = {
  streams: {
    flex: "1",
    display: "flex",
    background: "#23252f",
    position: "relative",
    minHeight: "0",

    "& ul": {
      flex: "1",
      overflow: "scroll",
      padding: "1em",
      margin: "0",
      listStyle: "none",
      fontSize: "12px",
      fontFamily: "Fira Mono",
      lineHeight: "1.6em",
      whiteSpace: "nowrap"
    },

    "& span": {
      color: "#747a88",
      display: "inline-block",
      marginRight: "15px",
      boxSizing: "border-box",
      userSelect: "none"
    },

    "& div": {
      display: "inline-block",
      margin: "0",
      color: "#f1f1f1",
      whiteSpace: "pre"
    }
  },
  fullScreen: {
    position: "fixed",
    top: "0",
    left: "0",
    right: "0",
    bottom: "0",
    margin: "0",
    zIndex: "99999"
  },
  fullScreenButton: {
    cursor: "pointer",
    color: "#fff",
    fontSize: "22px",
    position: "absolute",
    right: "20px",
    top: "10px",
    background: "rgba(35, 37, 47, 0.65)"
  },
  autoScrollButton: {
    cursor: "pointer",
    color: "#fff",
    fontSize: "22px",
    position: "absolute",
    right: "46px",
    top: "10px",
    background: "rgba(35, 37, 47, 0.65)"
  },
  activeAutoScroll: {
    color: "#66cb63"
  },
  DEBUG: {
    color: "#FFFF91 !important"
  },
  ERROR: {
    color: "#FF6C60 !important"
  }
};

export default injectSheet(styles)(StreamView);
