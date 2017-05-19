// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

import map from "lodash/map";
import includes from "lodash/includes";

import type { Job, Tag } from "../../datamodel";
import TagBullet from "./TagBullet";
import JobOverview from "./JobOverview";

type Props = {
  classes: any,
  className: any,
  allTags: { [string]: Tag },
  selectJob: (string) => void,
  selectFilterTag: (string) => void,
  deselectFilterTag: (string) => void,
  toggleFilterTag: (string) => void,
  selectedTags: string[],
  displayedJobsList: Job[]
};

class JobFilterForm extends React.Component {
  props: Props;

  constructor(props: Props) {
    super(props);
  }

  render() {
    const {
      classes,
      className,
      allTags,
      selectJob,
      selectedTags,
      displayedJobsList,
      toggleFilterTag
    }: Props = this.props;

    //handleKeyPress(head(jobsListDisplayed), selectJob)
    return (
      <div className={classNames(className, classes.container)}>
        <div className={classes.filterForm}>
          <div className="filterBar">
            <ul className="tagsList">
              {map(allTags, tag => (
                <li key={"tag" + tag.name}>
                  <TagBullet
                    name={tag.name}
                    color={tag.color}
                    onClick={() => toggleFilterTag(tag.name)}
                    active={includes(selectedTags, tag.name)}
                    verbose
                  />
                </li>
              ))}
            </ul>
          </div>
        </div>
        <ul className={classes.jobsList}>
          {map(displayedJobsList, job => (
            <li
              key={"job" + job.id}
              className="job"
              onClick={e => (e.stopPropagation(), selectJob(job.id))}
            >
              <JobOverview
                id={job.id}
                name={job.name}
                kind={job.kind}
                description={job.description}
                tags={map(job.tags, tagId => allTags[tagId])}
              />
            </li>
          ))}
        </ul>
      </div>
    );
  }
}

const styles = {
  container: {
    maxHeight: "24em",
    display: "flex",
    flexDirection: "column",
    borderTop: "#ECF1F5 1px solid",
    "& ul": { listStyleType: "none", margin: 0, padding: 0 }
  },

  jobsList: {
    overflow: "auto",
    display: "block",
    "& .job": {
      height: "2.5em",
      borderTop: "1px solid #ECF1F5",
      cursor: "pointer",
      lineHeight: "1em",
      padding: "0.8em 2em",
      "&:hover": {
        backgroundColor: "#ECF1F5"
      }
    },
    "&::-webkit-scrollbar-track": {
      borderRadius: "4px",
      boxShadow: "none",
      backgroundColor: "#7D8B99"
    },
    "&::-webkit-scrollbar": {
      borderRadius: "4px",
      width: "10px",
      backgroundColor: "#7D8B99"
    },
    "&::-webkit-scrollbar-thumb": {
      borderRadius: "4px",
      backgroundColor: "#2F3647"
    }
  },
  filterForm: {
    backgroundColor: "#FFF",
    "& .filterBar": {
      minHeight: "2.5em",
      display: "flex",
      alignItems: "center",
      justifyContent: "flex-end",
      flexWrap: "wrap",
      "& .tagsList": {
        "& li": {
          display: "inline-flex",
          marginRight: "0.3em"
        }
      }
    }
  }
};

export default injectSheet(styles)(JobFilterForm);
