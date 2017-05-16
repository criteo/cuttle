// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";
import { connect } from "react-redux";
import type { Job, Tag } from "../datamodel/workflow";
import * as Actions from "../actions";

import Fuse from "fuse.js";

import Icon from "./generic/Icon";
import SearchBox from "./generic/SearchBox";
import JobFilterForm from "./JobFilterForm";
import Spinner from "./generic/Spinner";

import map from "lodash/map";
import head from "lodash/head";
import last from "lodash/last";
import values from "lodash/values";
import filter from "lodash/filter";
import reject from "lodash/reject";
import intersection from "lodash/intersection";
import includes from "lodash/includes";

// Setup fuse for fuzzy match searches in the job picker
const fuseSearchSetup = allJobs => {
  const options = {
    shouldSort: true,
    threshold: 0.1,
    location: 0,
    distance: 100,
    maxPatternLength: 32,
    minMatchCharLength: 1,
    keys: [
      "id",
      "name",
      "description"
    ]
  };
  return new Fuse(allJobs, options);
}

type Props = {
  classes: any,
  className: any,
  selectedJobs: Job[],
  deselectJob: () => void,
  selectJob: () => void,

  openUserbar: () => void,
  userbarOpen: boolean,
  
  jobSearchInput: string,
  selectedTags: Tag[],
  changeJobSearchInput: () => void,
  selectFilterTag: () => void,
  deselectFilterTag: () => void,
  toggleFilterTag: () => void,
  
  isLoading: boolean,
  
  allTags: { [string]: Tag },
  allJobs: { [string]: Job }
};

const handleKeyPress = (selectedJobs, jobsList, jobSearchInput, deselectJob, selectJob) =>
  e => {
    switch(e.key) {
      case "Backspace":
        if (!jobSearchInput && selectedJobs.length > 0)
          deselectJob(last(selectedJobs).id);
        break;
      case "Enter":
        if (jobsList.length > 0)
          selectJob(head(jobsList).id);
        break;
    }
  };

class Userbar extends React.Component {
  props: Props;
  fuzzySearch: Fuse;
  
  constructor(props: Props) {
    super(props);
    this.fuzzySearch = fuseSearchSetup(values(props.allJobs));
  }

  componentWillReceiveProps(nextProps: Props) {
    // Initially the list is empty, when it is retrieved, we update the fuzzy matcher
    const jobs = values(nextProps.allJobs);
    if (jobs.length > 0)
      this.fuzzySearch = fuseSearchSetup(values(nextProps.allJobs));
  }

  filteredJobsList(jobSearchInput, selectedTags) {
    // If something is typed in the searchbox, we use it to filter
    const prefiltered = jobSearchInput
      ? this.fuzzySearch.search(jobSearchInput)
      : this.props.allJobs;

    // If a tag is selected at least, we filter (multiple tags selected reduce the selection)
    return selectedTags.length > 0
      ? filter(
        prefiltered,
        j => intersection(j.tags, selectedTags).length == selectedTags.length
      )
      : prefiltered;
  }
  
  render(){
    
    const {
      classes,
      className,
      selectedJobs,
      userbarOpen,
      openUserbar,
      selectedTags,
      
      deselectJob,
      selectJob,
      allTags,
      allJobs,

      selectFilterTag,
      deselectFilterTag,
      toggleFilterTag,
      jobSearchInput,
      changeJobSearchInput,
      
      isLoading
    }: Props = this.props;

    const filteredJobsList = this.filteredJobsList(jobSearchInput, selectedTags);
    const displayedJobsList = reject(filteredJobsList, j => includes(selectedJobs, j.id))
    const selectedJobsList = map(selectedJobs, jobId => allJobs[jobId]);
    
    return (
      <div
        className={classNames(className, classes.bar)}
        onClick={e => (e.stopPropagation(), openUserbar())}
      >
        <ul className={classes.selectors}>
          {map(selectedJobsList, job => (
             <li
               key={"job" + job.id}
               className={classes.jobBullet}
             >
               {job.name}
               <Icon
                 className={classes.closeIcon}
                 iconName="close"
                 onClick={e => (e.stopPropagation(), deselectJob(job.id))}
               />
             </li>
           )
           )}
        <li className="searchBox" key="searchBox">
          <SearchBox
            autoFocus={userbarOpen}
            defaultValue={jobSearchInput}
            onChange={e => changeJobSearchInput(e.target.value)}
            onKeyDown={handleKeyPress(selectedJobsList, values(displayedJobsList), jobSearchInput, deselectJob, selectJob)}
            placeholder="Pick a job..."
          />
        </li>
        </ul>
        { userbarOpen &&
          (isLoading
            ? <Spinner dark key="spinner-userbar" />
            : <JobFilterForm
                key="job-filter-form"
                displayedJobsList={displayedJobsList}
                allTags={allTags}
                selectJob={selectJob}
                selectedTags={selectedTags}
                selectFilterTag={selectFilterTag}
                deselectFilterTag={deselectFilterTag}
                toggleFilterTag={toggleFilterTag}
              />)
        }
      </div>
    );
  }
}

const styles = {
  bar: {
    position: "absolute",
    backgroundColor: "#FFF",
    color: "#BECBD6",
    boxShadow: "0px 1px 5px 0px #BECBD6",
    width: "100%",
    transition: "all 0.3s",
    "&:hover, &.active": {
      boxShadow: "0px 1px 5px 0px #36ABD6"
    },
    "& ul": {listStyleType: "none", margin: 0, padding: 0}
  },
  selectors: {
    position: "relative",
    display: "flex",
    alignItems: "center",
    minHeight: "4em",
    "&:first-child": {
      marginLeft: "0.5em"
    },
    "& .searchBox": {
      flexGrow: 1,
      margin: "auto",
      marginLeft: "0.5em"
    },
  },
  jobBullet: {
    color: "#2F3647",
    display: "inline-flex",
    alignItems: "center",
    lineHeight: "1em",
    fontSize: "0.9em",
    fontWeight: "bold",
    backgroundColor: "#E1EFFA",
    padding: "0.5em",
    marginLeft: "0.5em",
    borderRadius: "0.2em"
  },
  filterTextInput: {
    border: "none"
  },
  closeIcon: {
    color: "#2F3647",
    fontSize: "0.8em !important",
    marginLeft: "0.4em !important",
    fontWeight: "bold !important",
    cursor: "pointer"
  }
};

const mapStateToProps = ({ userbar }) => ({
  userbarOpen: userbar.open,
  selectedTags: userbar.selectedTags,
  selectedJobs: userbar.selectedJobs,
  jobSearchInput: userbar.jobSearchInput
});

const mapDispatchToProps = dispatch => ({
  selectJob: Actions.selectJob(dispatch),
  deselectJob: Actions.deselectJob(dispatch),
  
  toggleUserbar: Actions.toggleUserbar(dispatch),
  openUserbar: Actions.openUserbar(dispatch),
  closeUserbar: Actions.closeUserbar(dispatch),
  
  changeJobSearchInput: Actions.changeJobSearchInput(dispatch),
  selectFilterTag: Actions.selectFilterTag(dispatch),
  deselectFilterTag: Actions.deselectFilterTag(dispatch),
  toggleFilterTag: Actions.toggleFilterTag(dispatch)
});

export default connect(mapStateToProps, mapDispatchToProps)(
  injectSheet(styles)(Userbar)
);
