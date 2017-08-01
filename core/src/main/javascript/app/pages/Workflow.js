// @flow

import React from "react";
import injectSheet from "react-jss";
import ReactTooltip from "react-tooltip";
import { createClassFromLiteSpec } from "react-vega-lite";

import entries from "lodash/entries";
import filter from "lodash/filter";
import find from "lodash/find";
import flatMap from "lodash/flatMap";
import groupBy from "lodash/groupBy";
import includes from "lodash/includes";
import map from "lodash/map";
import mean from "lodash/mean";
import reduce from "lodash/reduce";
import some from "lodash/some";

import type { Node, Edge } from "../../graph/dagger/dataAPI/genericGraph";
import type { Workflow, Tag, Job, Dependency } from "../../datamodel";

import Select from "react-select";
import { navigate } from "redux-url";
import { connect } from "react-redux";
import { markdown } from "markdown";

import TagIcon from "react-icons/lib/md/label";
import Dagger from "../../graph/Dagger";
import SlidePanel from "../components/SlidePanel";
import FancyTable from "../components/FancyTable";

import moment from "moment";

type Props = {
  classes: any,
  workflow: Workflow,
  selectedJobs: string[],
  job: string,
  navTo: () => void
};

type State = {
  data: any[]
};

const AverageRunWaitChart = createClassFromLiteSpec("AverageRunWaitChart", {
  width: "550",
  title: "Runtime of jobs across time",
  mark: "area",
  transform: [
    {
      calculate: "datum.kind == 'run' ? 'Running' : 'Waiting'",
      as: "runningSeconds"
    }
  ],
  encoding: {
    x: {
      field: "startTime",
      type: "temporal",
      timeUnit: "utcyearmonthday",
      axis: {
        title: null,
        format: "%d/%m",
        labelAngle: -45
      }
    },
    y: {
      field: "seconds",
      type: "quantitative",
      aggregate: "sum",
      axis: {
        title: "Duration (s)"
      }
    },
    color: {
      type: "nominal",
      field: "runningSeconds",
      scale: {
        range: ["#00BCD4", "#ff9800"]
      },
      legend: { title: "Status" }
    }
  }
});

const MaxRuntimeChart = createClassFromLiteSpec("MaxRuntimeChart", {
  width: "550",
  mark: "line",
  transform: [
    {
      calculate: "datum.durationSeconds - datum.waitingSeconds",
      as: "runningSeconds"
    }
  ],
  encoding: {
    x: {
      field: "startTime",
      timeUnit: "utcyearmonthday",
      type: "temporal",
      axis: {
        title: null,
        format: "%d/%m",
        labelAngle: -45
      }
    },
    y: {
      aggregate: "max",
      type: "quantitative",
      field: "runningSeconds",
      axis: {
        title: "Max running time (s)"
      }
    }
  },
  config : {
    mark : {
      color : "#00BCD4"
    }
  }
});

const SumFailuresChart = createClassFromLiteSpec("SumFailuresChart", {
  width: "550",
  title: "Failures across time.",
  mark: "bar",
  transform: [
    { calculate: "datum.status === 'failed' ? 1 : 0", as: "failures" }
  ],
  encoding: {
    x: {
      field: "startTime",
      timeUnit: "utcyearmonthday",
      type: "temporal",
      axis: {
        format: "%d/%m",
        title: null,
        labelAngle: -45
      }
    },
    y: {
      type: "quantitative",
      aggregate: "sum",
      field: "failures",
      axis: {
        title: "Number of failures"
      }
    }
  },
  config : {
    mark : {
      color : "#e91e63"
    }
  }
});

type ExecutionStat = {
  startTime: string,
  durationSeconds: number,
  waitingSeconds: number,
  status: "successful" | "failure"
};

/**
 * Unpivots execution stats to create 
 * separate "run" & "wait" events aggregated
 * by date.
 */
const aggregateDataSet = (data: ExecutionStat[]) =>
  flatMap(
    entries(
      groupBy(data, d =>
        moment(d.startTime)
          .set({
            hour: 0,
            minute: 0,
            second: 0,
            millisecond: 0
          })
          .format()
      )
    ),
    ([k, v]) => [
      {
        startTime: k,
        kind: "run",
        seconds: mean(v.map(x => x.durationSeconds - x.waitingSeconds))
      },
      {
        startTime: k,
        kind: "wait",
        seconds: mean(v.map(x => x.waitingSeconds))
      }
    ]
  );

class WorkflowComponent extends React.Component {
  props: Props;
  state: State;

  constructor(props) {
    super(props);

    this.updateCharts(props);

    this.state = {
      data: []
    };
  }

  componentWillReceiveProps(nextProps: Props) {
    this.updateCharts(nextProps);
  }

  updateCharts(nextProps: Props) {
    const chartJob = nextProps.job || nextProps.workflow.jobs[0].id;
    fetch(`/api/statistics/${chartJob}`).then(data => data.json()).then(json => {
      this.setState({
        data: json
      });
    });
  }

  render() {
    const {
      classes,
      workflow = {},
      job,
      selectedJobs = [],
      navTo
    } = this.props;

    const filteredJobs = filter(workflow.jobs, j =>
      includes(selectedJobs, j.id)
    );
    const jobs = filteredJobs.length > 0 ? filteredJobs : workflow.jobs;
    const nodes: Node[] = map(jobs, (j: Job, i) => ({
      ...j,
      order: i,
      yPosition: i
    }));

    const filteredEdges = filter(
      workflow.dependencies,
      e => some(jobs, { id: e.from }) && some(jobs, { id: e.to })
    );
    const edges: Edge[] = map(filteredEdges, (d: Dependency) => ({
      id: d.from + d.to,
      source: d.from,
      target: d.to,
      value: 1
    }));

    const tagsDictionnary = reduce(
      workflow.tags,
      (acc, current) => ({
        ...acc,
        [current.name]: current
      }),
      {}
    );

    const startNode = find(jobs, { id: job }) || jobs[0];

    ReactTooltip.rebuild();

    const renderTimeSeriesSechduling = () => [
      startNode.scheduling.grid && [
        <dt key="period">Period:</dt>,
        <dd key="period_">{startNode.scheduling.grid.period}</dd>
      ],
      startNode.scheduling.start && [
        <dt key="start">Start Date:</dt>,
        <dd key="start_">{startNode.scheduling.start}</dd>
      ],
      startNode.scheduling.maxPeriods != 1 && [
        <dt key="maxPeriods">Max Periods:</dt>,
        <dd key="maxPeriods_">{startNode.scheduling.maxPeriods}</dd>
      ]
    ];

    return (
      <div className={classes.main}>
        <Dagger
          nodes={nodes}
          edges={edges}
          tags={workflow.tags}
          startNodeId={startNode.id}
          onClickNode={id => navTo("/workflow/" + id)}
        />
        <Select
          className={classes.jobSelector}
          name="jobSelector"
          options={map(nodes, n => ({ value: n.id, label: n.name }))}
          onChange={o => navTo("/workflow/" + o.value)}
        />
        <SlidePanel>
          <div className={classes.jobCard}>
            <FancyTable>
              <dt key="id">Id:</dt>
              <dd key="id_">
                {startNode.id}
              </dd>
              <dt key="name">Name:</dt>
              <dd key="name_">
                {startNode.name}
              </dd>
              {renderTimeSeriesSechduling()}
              {startNode.tags.length > 0 && [
                <dt key="tags">Tags:</dt>,
                <dd key="tags_" className={classes.tags}>
                  {map(startNode.tags, t => [
                    <span
                      key={tagsDictionnary[t].name}
                      className={classes.tag}
                      data-for={"tag" + tagsDictionnary[t].name}
                      data-tip={tagsDictionnary[t].description}
                    >
                      <TagIcon className="tagIcon" />
                      {tagsDictionnary[t].name}
                    </span>,
                    <ReactTooltip
                      id={"tag" + tagsDictionnary[t].name}
                      effect="float"
                    />
                  ])}
                </dd>
              ]}
              {startNode.description && [
                <dt key="description">Description:</dt>,
                <dd
                  key="description_"
                  className={classes.description}
                  dangerouslySetInnerHTML={{
                    __html: markdown.toHTML(startNode.description)
                  }}
                />
              ]}
            </FancyTable>
          </div>
          <div className={classes.chartSection}>
            <h3>Average run/wait times over last 30 days</h3>
            <AverageRunWaitChart
              className="chart"
              data={{ values: aggregateDataSet(this.state.data) }}
            />
          </div>
          <div className={classes.chartSection}>
            <h3>Max runtime over last 30 days</h3>
            <MaxRuntimeChart
              className="chart"
              data={{ values: this.state.data }}
            />
          </div>
          <div className={classes.chartSection}>
            <h3>Number of failures over last 30 days</h3>
            <SumFailuresChart
              className="chart"
              data={{ values: this.state.data }}
            />
          </div>
        </SlidePanel>
      </div>
    );
  }
}

const styles = {
  main: {
    backgroundColor: "#ECF1F5",
    flex: 1,
    width: "100%",
    height: "calc(100vh - 4em)",
    position: "relative"
  },
  chartSection: {
    "& > .chart": {
      marginLeft: "50px",
      marginBottom: "50px"
    },
    "& h3": {
      color: "#3B4254",
      textAlign: "center",
      fontSize: "1em"
    }
  },
  tags: {
    display: "table-cell"
  },
  tag: {
    cursor: "help",
    verticalAlign: "middle",
    border: "1px solid #999",
    margin: "0 0.2em",
    padding: "0.2em 0.4em",
    borderRadius: "0.2em",
    "& .tagIcon": {
      marginRight: "0.4em",
      fontSize: "1.2em"
    }
  },
  description: {
    lineHeight: "1.25em !important",
    fontSize: "0.95em",
    textAlign: "justify !important",
    overflowY: "scroll"
  },
  jobSelector: {
    position: "absolute",
    top: "2em",
    left: "50%",
    marginLeft: "-300px",
    width: "600px",
    "& .Select-control": {
      height: "1em",
      backgroundColor: "#F5F8FA",
      "& .Select-value": {
        color: "#A9B8C3",
        fontSize: "0.9em"
      },
      "& .Select-menu ! important": {
        margin: "0 1em",
        width: "calc(600px - 2em)"
      },
      "& .Select-menu-outer !important": {
        margin: "0 1em",
        width: "calc(600px - 2em)"
      },
      "& .Select-option !important": {
        fontSize: "0.9em"
      },
      "& .Select-arrow-zone": {
        display: "none"
      }
    }
  },
  jobCard: {
    color: "#3B4254"
  }
};

export default connect(
  () => ({}),
  dispatch => ({
    navTo: link => dispatch(navigate(link))
  })
)(injectSheet(styles)(WorkflowComponent));
