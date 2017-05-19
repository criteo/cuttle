// @flow

import React from "react";
import moment from "moment";

type Props = {
  className: string,
  time: string
};

class Clock extends React.Component {
  props: Props;
  timer: ?any;

  componentWillMount() {
    this.timer = setInterval(() => this.forceUpdate(), 15 * 1000);
  }

  componentWillUnmount() {
    this.timer && clearInterval(this.timer);
  }

  render() {
    let { className, time } = this.props;
    return <span className={className}>{moment(time).fromNow()}</span>;
  }
}

export default Clock;
