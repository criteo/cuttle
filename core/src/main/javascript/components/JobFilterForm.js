// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
//import Transition from "react-motion-ui-pack";
import map from "lodash/map";
import keys from "lodash/keys";
import includes from "lodash/includes";

import type { Job, Tag } from "../datamodel/workflow";

type Props = {
  classes: any,
  className: any,
  allJobs: { [string]: Job },
  allTags: { [string]: Tag },
  selectJob: (string) => void,
  deselectJob: (string) => void,
  selectedJobs: string[]
};

class JobFilterForm extends React.Component {
  props: Props;
  state: {
    filteredJobs: string[]
  };

  constructor(props: Props) {
    super(props);
    this.state = {
      filteredJobs: keys(props.allJobs)
    };
  }

  toggleSelectJob(jobId, selectedJobs, selectJob, deselectJob) {
    return includes(selectedJobs, jobId)
      ? (e => (e.stopPropagation(), deselectJob(jobId)))
      : (e => (e.stopPropagation(), selectJob(jobId)));
  }

  render() {
    const { filteredJobs } = this.state;
    const { classes, className, allJobs, allTags, selectJob, deselectJob, selectedJobs }: Props = this.props;
    return (
      <div className={classNames(className, classes.container)}>
        <div className={classes.jobsList}>
          {map(filteredJobs, jobId => (
             <div
               key={"job" + jobId}
               className={classNames(classes.job, includes(selectedJobs, jobId) && "selected")}
               onClick={this.toggleSelectJob(jobId, selectedJobs, selectJob, deselectJob)}
             >
               <div className={classes.jobName}>
                 {allJobs[jobId].name || jobId}
               </div>
               <div className={classes.jobTags}>
                 {map(allJobs[jobId].tags, tagName => (
                    <div
                      key={tagName}
                      className={classes.jobTag}
                      style={{backgroundColor: allTags[tagName] && allTags[tagName].color}}
                    />
                  )) }
               </div>
               <div className={classes.jobDescription}>
                 {allJobs[jobId].description}
               </div>
               
             </div>
           ))}
        </div>
        
        <div className={classes.filterForm}>
        </div>
      </div>
    );
  }
}

const styles = {
  container: {
    height: "300px",
    display: "flex",
    flexDirection: "row"
  },
  job: {
    cursor: "pointer",
    lineHeight: "1em",
    padding: "0.8em",
    flex: "1",
    "&:hover, &.selected": {
      backgroundColor: "#ECF1F5"
    },
    borderTop: "1px solid #ECF1F5"
  },
  jobName: {
    float: "left",
    color: "#2F3647",
    lineHeight: "1.1em"
  },
  jobDescription: {
    clear: "left",
    float: "left",
    color: "#7D8B99",
    fontSize: "0.8em"
  },
  jobTags: {
  },
  jobTag: {
    width: "1em",
    height: "1em",
    borderRadius: "0.2em",
    float: "right",
    margin: "0 0.2em",
    lineHeight: "1.1em"
  },
  jobsList: {
    height: "inherit",
    overflow: "auto",
    display: "flex",
    flexDirection: "column",
    flex: "1",
    "&::-webkit-scrollbar-track": {
      borderRadius: "3px",
      boxShadow: "none",
      backgroundColor: "#7D8B99"
    },
    "&::-webkit-scrollbar": {
      borderRadius: "3px",
      width: "6px",
      backgroundColor: "#7D8B99"
    },
    "&::-webkit-scrollbar-thumb": {
      borderRadius: "3px",
      backgroundColor: "#2F3647"
    }
  },
  filterForm: {
    flex: "1",
    backgroundColor: "#FFF",
    display: "flex"
  }
};
export default injectSheet(styles)(JobFilterForm);
