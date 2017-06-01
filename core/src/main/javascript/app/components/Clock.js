// @flow

import React from "react";
import moment from "moment";

type Props = {
  className?: string,
  time: string,
  humanize?: boolean
};

class Clock extends React.Component {
  props: Props;
  timer: ?any;

  componentWillMount() {
    this.timer = setInterval(
      () => this.forceUpdate(),
      this.props.humanize ? 15 * 1000 : 1000
    );
  }

  componentWillUnmount() {
    this.timer && clearInterval(this.timer);
  }

  render() {
    let { className, time, humanize = true } = this.props;
    return (
      <span className={className}>
        {humanize
          ? moment(time).fromNow()
          : moment.utc(moment().diff(moment(time))).format("HH:mm:ss")}
      </span>
    );
  }
}

export default Clock;
