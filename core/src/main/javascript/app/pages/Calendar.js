// @flow

import React from "react";
import ReactDOM from "react-dom";
import { connect } from "react-redux";
import classNames from "classnames";
import { navigate } from "redux-url";
import injectSheet from "react-jss";
import moment from "moment";
import _ from "lodash";

import { Calendar as MiniCalendar } from "react-calendar";
import Spinner from "../components/Spinner";

import { listenEvents } from "../../Utils";

type Props = {
  classes: any,
  envCritical: boolean,
  selectedJobs: Array<string>,
  drillDown: (date: any) => void
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
    let jobsFilter = props.selectedJobs.length
      ? `&jobs=${props.selectedJobs.join(",")}`
      : "";
    let query = `/api/timeseries/calendar?events=true${jobsFilter}`;
    if (this.state.query != query) {
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
    let { classes, drillDown, envCritical } = this.props;
    let { data } = this.state;
    return (
      <div
        className={classNames(classes.container, {
          [classes.critical]: envCritical
        })}
      >
        {data
          ? <MiniCalendar
              weekNumbers={false}
              startDate={moment(data[0].date).startOf("month")}
              endDate={moment(data[data.length - 1].date).endOf("month")}
              mods={data
                .map(({ date, completion, stuck }) => {
                  return {
                    date: moment(date),
                    classNames: [
                      stuck
                        ? "stuck"
                        : completion == 1
                            ? "done"
                            : completion == 0
                                ? "todo"
                                : `progress-${completion
                                    .toString()
                                    .substring(2)}`
                    ],
                    component: ["day"]
                  };
                })
                .concat([
                  {
                    events: {
                      onClick: drillDown
                    },
                    component: ["day"]
                  }
                ])}
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
      width: "27px",
      height: "15px",
      lineHeight: "15px",
      textAlign: "center",
      cursor: "pointer",
      border: "4px solid #fff",
      marginLeft: "1px",
      marginRight: "1px",
      marginBottom: "10px",
      position: "relative",
      "&::after": {
        content: "''",
        position: "absolute",
        left: "-4px",
        right: "-4px",
        height: "3px",
        background: "#26a69a",
        bottom: "-8px",
        display: "none"
      }
    },
    "& .rc-Day:hover": {
      "&::after": {
        display: "block"
      }
    },
    "& .rc-Day--outside": {
      cursor: "default",
      visibility: "hidden"
    },
    "& .rc-Day--outside:hover": {
      backgroundColor: "transparent"
    },
    "& .rc-Day--done": {
      borderColor: "#62cc64",
      backgroundColor: "#62cc64",
      color: "#fff"
    },
    "& .rc-Day--stuck": {
      backgroundColor: "#e91e63",
      borderColor: "#e91e63",
      color: "#fff"
    },
    "& .rc-Day--progress-9": {
      borderColor: "rgba(98, 204, 100, 1)",
      backgroundColor: "#ecf1f5"
    },
    "& .rc-Day--progress-8": {
      borderColor: "rgba(98, 204, 100, 0.9)",
      backgroundColor: "#ecf1f5"
    },
    "& .rc-Day--progress-7": {
      borderColor: "rgba(98, 204, 100, 0.8)",
      backgroundColor: "#ecf1f5"
    },
    "& .rc-Day--progress-6": {
      borderColor: "rgba(98, 204, 100, 0.7)",
      backgroundColor: "#ecf1f5"
    },
    "& .rc-Day--progress-5": {
      borderColor: "rgba(98, 204, 100, 0.6)",
      backgroundColor: "#ecf1f5"
    },
    "& .rc-Day--progress-4": {
      borderColor: "rgba(98, 204, 100, 0.5)",
      backgroundColor: "#ecf1f5"
    },
    "& .rc-Day--progress-3": {
      borderColor: "rgba(98, 204, 100, 0.4)",
      backgroundColor: "#ecf1f5"
    },
    "& .rc-Day--progress-2": {
      borderColor: "rgba(98, 204, 100, 0.3)",
      backgroundColor: "#ecf1f5"
    },
    "& .rc-Day--progress-1": {
      borderColor: "rgba(98, 204, 100, 0.2)",
      backgroundColor: "#ecf1f5"
    },
    "& .rc-Day--todo": {
      borderColor: "#ecf1f5",
      backgroundColor: "#ecf1f5"
    }
  },
  critical: {
    "& .rc-Day::after": {
      background: "#ff5722 !important"
    }
  }
};

const mapStateToProps = ({ app: { selectedJobs, project } }) => ({
  selectedJobs,
  envCritical: project.env.critical
});
const mapDispatchToProps = dispatch => ({
  drillDown(date) {
    let day = moment.utc(date.format("YYYY-MM-DD"));
    let f = "YYYY-MM-DDTHH";
    dispatch(
      navigate(
        `/timeseries/calendar/${day.format(f)}Z_${day.add(1, "day").format(f)}Z`
      )
    );
  }
});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(Calendar)
);
