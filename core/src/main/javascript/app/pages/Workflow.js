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

import type { Edge, Node } from "../../graph/dagger/dataAPI/genericGraph";
import type { Dependency, Job, Workflow } from "../../datamodel";

import Select from "react-select";
import { navigate } from "redux-url";
import { connect } from "react-redux";
import { markdown } from "markdown";

import TagIcon from "react-icons/lib/md/label";
import MdList from "react-icons/lib/md/list";
import Dagger from "../../graph/Dagger";
import Window from "../components/Window";
import FancyTable from "../components/FancyTable";
import Spinner from "../components/Spinner";
import Link from "../components/Link";

import moment from "moment";
import PopoverMenu from "../components/PopoverMenu";
import { Badge } from "../components/Badge";
import Status from "../components/Status";

type Props = {
  classes: any,
  workflow: Workflow,
  selectedJobs: string[],
  job: string,
  navTo: () => void,
  showDetail: boolean,
  refPath?: string
};

type User = {
  userId: string
};

type PausedJob = {
  id: string,
  user: User,
  date: string
};

type State = {
  data: ?(any[]),
  pausedJobs: ?{[string]: PausedJob}
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
  config: {
    mark: {
      color: "#00BCD4"
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
  config: {
    mark: {
      color: "#e91e63"
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
    this.updatePausedJobs();

    this.state = {
      data: undefined,
      pausedJobs: undefined
    };
  }

  componentWillReceiveProps(nextProps: Props) {
    if (nextProps && nextProps.job && nextProps.job !== this.props.job) {
      this.setState({
        data: undefined
      });
      const jobId = nextProps.job || nextProps.workflow.jobs[0].id;
      this.updateCharts(jobId);
    }
  }

  updateCharts(jobId: string) {
    fetch(`/api/statistics/${jobId}`).then(data => data.json()).then(json => {
      this.setState({
        data: json
      });
    });
  }

  updatePausedJobs() {
    fetch(`/api/jobs/paused`).then(data => data.json()).then(json => {
      this.setState({
        pausedJobs: json.reduce(
          (acc, job) => ({...acc, [job.id]: job }),
          {}
        )
      });
    });
  }

  render() {
    const {
      classes,
      workflow = {},
      job,
      selectedJobs = [],
      navTo,
      showDetail,
      refPath
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

    const renderScheduling = () =>
      (startNode.scheduling.kind == 'timeseries') ?
        [
          startNode.scheduling.calendar && [
            <dt key="period">Period:</dt>,
            <dd key="period_">{startNode.scheduling.calendar.period}</dd>
          ],
          startNode.scheduling.start && [
            <dt key="start">Start Date:</dt>,
            <dd key="start_">{startNode.scheduling.start}</dd>
          ],
          startNode.scheduling.end && [
            <dt key="end">End Date:</dt>,
            <dd key="end_">{startNode.scheduling.end}</dd>
          ],
          startNode.scheduling.maxPeriods > 1 && [
            <dt key="maxPeriods">Max Periods:</dt>,
            <dd key="maxPeriods_">{startNode.scheduling.maxPeriods}</dd>
          ]
        ] :
        [
          <dt key="scheduling">Scheduling:</dt>,
          <dd key="scheduling_">{JSON.stringify(startNode.scheduling)}</dd>
        ];

    const charts = (data: any) => {
      if (data) {
        return (
          <div className={classes.charts}>
            <div className={classes.chartSection}>
              <h3>Average run/wait times over last 30 days</h3>
              <AverageRunWaitChart
                className="chart"
                data={{ values: aggregateDataSet(data) }}
              />
            </div>
            <div className={classes.chartSection}>
              <h3>Max runtime over last 30 days</h3>
              <MaxRuntimeChart className="chart" data={{ values: data }} />
            </div>
            <div className={classes.chartSection}>
              <h3>Number of failures over last 30 days</h3>
              <SumFailuresChart className="chart" data={{ values: data }} />
            </div>
          </div>
        );
      }
      return (
        <div style={{ textAlign: "center" }}>
          <div style={{ display: "inline-block", marginTop: "50px" }}>
            <Spinner />
          </div>
        </div>
      );
    };

    const JobMenu = ({ job }: { job: string }) => {
      const menuItems = this.state.pausedJobs && this.state.pausedJobs[job]
        ? [
            <span
              onClick={() =>
                fetch(`/api/jobs/resume?jobs=${job}`, {
                  method: "POST",
                  credentials: "include"
                }).then(() => this.updatePausedJobs(job))}
            >
              Resume
            </span>
          ]
        : [
            <span
              onClick={() =>
                fetch(`/api/jobs/pause?jobs=${job}`, {
                  method: "POST",
                  credentials: "include"
                }).then(() => this.updatePausedJobs(job))}
            >
              Pause
            </span>
          ];

      return <PopoverMenu className={classes.menu} items={menuItems} />;
    };

    const daggerTags =
      this.state.pausedJobs &&
      workflow.jobs.reduce((acc: {}, job: Job) => {
        if (this.state.pausedJobs && this.state.pausedJobs[job.id]) {
          return {...acc, [job.id]: "#FFAAFF"};
        } else {
          return {...acc, [job.id]: "#E1EFFA"};
        }
      }, {});

    return (
      <div className={classes.main}>
        <Dagger
          nodes={nodes}
          edges={edges}
          tags={daggerTags}
          startNodeId={startNode.id}
          onClickNode={id => navTo("/workflow/" + id)}
        />
        <div className={classes.controller}>
          <Select
            className={classes.jobSelector}
            name="jobSelector"
            options={map(nodes, n => ({ value: n.id, label: n.name }))}
            onChange={o => navTo("/workflow/" + o.value)}
            value={startNode.id}
            clearable={false}
          />
          <Link
            className={classes.detailIcon}
            title="Job details"
            href={`/workflow/${startNode.id}?showDetail=true`}
          >
            <MdList />
          </Link>
        </div>
        {showDetail &&
          <Window closeUrl={refPath} title="Job details">
            <div className={classes.jobCard}>
              <JobMenu job={startNode.id} />
              <FancyTable>
                <dt key="id">Id:</dt>
                <dd key="id_">{startNode.id}</dd>
                <dt key="name">Name:</dt>
                <dd key="name_">{startNode.name}</dd>
                {renderScheduling()}
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
                {this.state.pausedJobs &&
                  (this.state.pausedJobs[startNode.id] ? [
                  <dt key="status">Status:</dt>,
                  <dd key="status_">
                    <Status status="paused" /> &nbsp;by&nbsp;
                    {this.state.pausedJobs[startNode.id].user.userId}&nbsp;
                    {moment.utc(this.state.pausedJobs[startNode.id].date).fromNow()}
                  </dd>
                ] : [
                  <dt key="status">Status:</dt>,
                  <dd key="status_">
                    <Badge
                      label="Active"
                      width={75}
                      light={true}
                      kind="info"
                    />
                  </dd>
                ])}
              </FancyTable>
            </div>
            {charts(this.state.data)}
          </Window>}
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
  charts: {
    overflow: "auto",
    display: "flex",
    flexWrap: "wrap",
    justifyContent: "space-around"
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
  controller: {
    position: "absolute",
    top: "2em",
    display: "flex",
    justifyContent: "center",
    width: "100%"
  },
  detailIcon: {
    fontSize: "30px",
    color: "#607e96",
    marginLeft: ".25em",
    cursor: "pointer"
  },
  jobSelector: {
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
    color: "#3B4254",
    position: "relative"
  },
  menu: {
    position: "absolute",
    top: "10px",
    right: "1em"
  }
};

const mapStateToProps = ({ app: { page: { showDetail, refPath } } }) => ({ showDetail, refPath });

export default connect(mapStateToProps, dispatch => ({
  navTo: link => dispatch(navigate(link))
}))(injectSheet(styles)(WorkflowComponent));
