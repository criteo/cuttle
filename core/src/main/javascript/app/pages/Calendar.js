// @flow

import React from "react";
import ReactDOM from "react-dom";
import { connect } from "react-redux";
import classNames from "classnames";
import injectSheet from "react-jss";
import moment from "moment";
import _ from "lodash";

import { Calendar as MiniCalendar } from "react-calendar";
import Spinner from "../components/Spinner";

import { listenEvents } from "../../Utils";

type Props = {
  classes: any,
  selectedJobs: Array<string>
};

type Day = {
  date: string,
  completion: number,
  stuck?: boolean
};

type State = {
  data: ?Array<Day>,
  query: ?string,
  eventSource: ?any
};

class Calendar extends React.Component {
  props: Props;
  state: State;

  constructor(props: Props) {
    super(props);
    this.state = {
      data: null,
      query: null,
      eventSource: null
    };
  }

  listenForUpdates(props: Props) {
    let jobsFilter = props.selectedJobs.length ? `&jobs=${props.selectedJobs.join(",")}` : "";
    let query = `/api/timeseries/calendar?events=true${jobsFilter}`;
    if(this.state.query != query) {
      this.state.eventSource && this.state.eventSource.close();
      let eventSource = listenEvents(query, this.updateData.bind(this));
      this.setState({
        ...this.state,
        data: null,
        eventSource
      });
    }
  }

  componentDidMount() {
    let scroller: any = ReactDOM.findDOMNode(this);
    scroller.scrollTop = Number.MAX_SAFE_INTEGER;
    this.listenForUpdates(this.props);
  }

  componentWillReceiveProps(nextProps: Props) {
    this.listenForUpdates(nextProps);
  }

  componentWillUnmount() {
    let { eventSource } = this.state;
    eventSource && eventSource.close();
  }

  updateData(json: Array<Day>) {
    this.setState({
      ...this.state,
      data: json
    });
  }

  render() {
    let { classes } = this.props;
    let { data } = this.state;
    return (
      <div className={classes.container}>
        {data
          ? <MiniCalendar
              weekNumbers={false}
              startDate={moment(data[0].date).startOf("month")}
              endDate={moment(data[data.length - 1].date).endOf("month")}
              mods={data.map(({ date, completion, stuck }) => {
                return {
                  date: moment(date),
                  classNames: [
                    stuck
                      ? "stuck"
                      : completion == 1
                          ? "done"
                          : completion == 0
                              ? "todo"
                              : `progress-${completion.toString().substring(2)}`
                  ],
                  component: ["day"]
                };
              })}
            />
          : <Spinner />}
      </div>
    );
  }
}

const styles = {
  container: {
    overflow: "scroll",
    display: "flex",
    flex: "1",

    "& > div": {
      display: "flex",
      flexWrap: "wrap",
      justifyContent: "flex-start",
      fontSize: ".85em",
      padding: "1em 0"
    },
    "& .rc-Calendar-header": {
      display: "none"
    },
    "& .rc-Month": {
      flex: "0 1 auto",
      margin: "1em 0 1em 2em",
      background: "#fff",
      padding: ".5em",
      maxWidth: "calc((100% - 8em) / 3)",
      borderRadius: "2px",
      height: "225px",
      minWidth: "275px",
      boxShadow: "0px 1px 2px #BECBD6"
    },
    "& .rc-Month-header": {
      textAlign: "left",
      fontWeight: "600",
      padding: "1em",
      color: "#617483"
    },
    "& .rc-Month-weekdays": {
      display: "none"
    },
    "& .rc-Week": {},
    "& .rc-Week-days": {
      display: "inline-block"
    },
    "& .rc-Day": {
      display: "inline-block",
      fontSize: "13px",
      width: "29px",
      height: "15px",
      lineHeight: "15px",
      padding: "5px",
      textAlign: "center",
      cursor: "pointer",
      borderRadius: "1px",
      borderTop: "2.5px solid white",
      borderBottom: "2.5px solid white",
      position: "relative",
      "&::after": {
        content: "''",
        position: "absolute",
        left: "0",
        right: "0",
        height: "1px",
        background: "white",
        bottom: "1px"
      },
      "&::before": {
        content: "''",
        position: "absolute",
        left: "0",
        right: "0",
        height: "1px",
        background: "white",
        top: "0px"
      }
    },
    "& .rc-Day:hover": {
      borderBottom: "2.5px solid #ff5722"
    },
    "& .rc-Day--outside": {
      cursor: "default",
      visibility: "hidden"
    },
    "& .rc-Day--outside:hover": {
      backgroundColor: "transparent"
    },
    "& .rc-Day--done": {
      backgroundColor: "#62cc64",
      color: "#fff"
    },
    "& .rc-Day--stuck": {
      backgroundColor: "#e91e63",
      color: "#fff"
    },
    "& .rc-Day--progress-9": {
      backgroundColor: "rgba(0, 188, 212, 1)",
      color: "#fff"
    },
    "& .rc-Day--progress-8": {
      backgroundColor: "rgba(0, 188, 212, 0.9)",
      color: "#fff"
    },
    "& .rc-Day--progress-7": {
      backgroundColor: "rgba(0, 188, 212, 0.8)",
      color: "#fff"
    },
    "& .rc-Day--progress-6": {
      backgroundColor: "rgba(0, 188, 212, 0.7)",
      color: "#fff"
    },
    "& .rc-Day--progress-5": {
      backgroundColor: "rgba(0, 188, 212, 0.6)",
      color: "#fff"
    },
    "& .rc-Day--progress-4": {
      backgroundColor: "rgba(0, 188, 212, 0.5)",
      color: "#fff"
    },
    "& .rc-Day--progress-3": {
      backgroundColor: "rgba(0, 188, 212, 0.4)"
    },
    "& .rc-Day--progress-2": {
      backgroundColor: "rgba(0, 188, 212, 0.3)"
    },
    "& .rc-Day--progress-1": {
      backgroundColor: "rgba(0, 188, 212, 0.2)"
    },
    "& .rc-Day--todo": {
      backgroundColor: "#f2f9ff"
    }
  }
};

const mapStateToProps = ({selectedJobs}) => ({selectedJobs});
const mapDispatchToProps = dispatch => ({});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(Calendar)
);
