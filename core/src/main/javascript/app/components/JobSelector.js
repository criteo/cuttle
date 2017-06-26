// @flow

import React from "react";
import injectSheet from "react-jss";
import classNames from "classnames";
import _ from "lodash";
import unorm from "unorm";
import SearchIcon from "react-icons/lib/md/search";
import JobIcon from "react-icons/lib/go/git-commit";
import GraphIcon from "react-icons/lib/go/repo-forked";
import TagIcon from "react-icons/lib/md/label";

import Select from "react-select";
import type { Workflow } from "../../datamodel.js";

type Props = {
  classes: any,
  className?: string,
  workflow: Workflow,
  placeholder?: any,
  onChange: (jobs: Array<string>) => void,
  selected: Array<string>
};

type State = {
  selected: Array<Option>
};

type Option = {
  value: string,
  label: string,
  kind?: "job" | "parents" | "children" | "tag",
  job?: string,
  others?: Array<string>
};

class JobSelector extends React.Component {
  props: Props;
  state: State;
  options: Array<Option>;

  constructor(props: Props) {
    super(props);
    this.state = {
      focused: false,
      selected: props.selected.map(job => ({ value: job, label: job }))
    };
    this.options = _.flatMap(props.workflow.tags, ({ name }) => {
      let tagged = props.workflow.getTagged(name);
      if (tagged.length) {
        return [
          {
            value: `_${name}-TAG`,
            others: tagged,
            label: `Jobs tagged '${name}'`,
            kind: "tag"
          }
        ];
      }
      return [];
    }).concat(
      _.flatMap(_.sortBy(props.workflow.jobs, job => job.id), job => {
        let result = [
          { value: `_${job.id}`, job: job.id, label: job.name, kind: "job" }
        ];
        let parents = props.workflow.getParents(job.id);
        let children = props.workflow.getChildren(job.id);
        if (parents.length > 0) {
          result.push({
            value: `_${job.id}-PARENTS`,
            job: job.id,
            others: parents,
            label: `${job.name} and its dependencies`,
            kind: "parents"
          });
        }
        if (children.length > 0) {
          result.push({
            value: `_${job.id}-CHILDREN`,
            job: job.id,
            others: children,
            label: `${job.name} and jobs depending on it`,
            kind: "children"
          });
        }
        return result;
      })
    );
  }

  onSelectItem(selected: Array<Option>) {
    let newSelected = _.uniqBy(
      _.flatMap(selected, ({ value, label, job, others }) => {
        if (others && others.length) {
          return (job ? [{ value: job, label: job }] : []).concat(
            others.map(job => ({ value: job, label: job }))
          );
        } else if (job) {
          return [{ value: job, label: job }];
        }
        return [{ value, label }];
      }),
      o => o.value
    );
    this.setState({
      selected: newSelected
    });
    this.props.onChange(newSelected.map(s => s.value));
  }

  render() {
    let { className, classes, placeholder } = this.props;
    let { selected } = this.state;

    let renderOption = ({ value, label, kind, others }: Option) => (
      <span>
        {kind == "parents"
          ? <GraphIcon
              className={classes.optionIcon}
              style={{ transform: "rotate(-90deg) translateX(2px)" }}
            />
          : kind == "children"
          ? <GraphIcon
              className={classes.optionIcon}
              style={{ transform: "rotate(90deg) translateX(-2px)" }}
            />
          : kind == "tag"
          ? <TagIcon
              className={classes.optionIcon}
            />
          : <JobIcon className={classes.optionIcon} />}
        {label}
        {others && others.length > 0
          && <em className={classes.more}>
                {`${kind != "tag" ? "+" : ""}${others.length} job${others.length > 1 ? "s" : ""}`}
          </em>}
      </span>
    );

    let filterOptions = (
      options: Array<Option>,
      filter: string,
      currentValues: Array<Option>
    ) => {
      let tokenize = (text: string) =>
        unorm
          .nfkd(text)
          .replace(/[\u0300-\u036F]/g, "")
          .replace(/\W/g, " ")
          .toLowerCase()
          .split(" ");
      let currentJobs = currentValues.map(v => v.value);
      let availableOptions = options.filter(o => {
        let jobAlreadySelected = _.indexOf(currentJobs, o.job) > -1;
        let allOthersAlreadySelected =
          o.others &&
          o.others.length &&
          o.others.every(j => _.indexOf(currentJobs, j) > -1);
        return !jobAlreadySelected && !allOthersAlreadySelected;
      });
      let filterTokens = tokenize(filter);
      let filteredOptions = availableOptions.filter(({ label }) => {
        let labelTokens = tokenize(label);
        return _.every(filterTokens, t =>
          _.some(labelTokens, l => l.indexOf(t) > -1)
        );
      });
      return filteredOptions;
    };

    return (
      <Select
        multi
        joinValues
        placeholder={
          placeholder ||
            <span>
              <SearchIcon className={classes.searchIcon} /> Select jobs...
            </span>
        }
        className={classNames(className, classes.select)}
        value={selected}
        options={this.options}
        onChange={this.onSelectItem.bind(this)}
        optionRenderer={renderOption}
        filterOptions={filterOptions}
      />
    );
  }
}

const styles = {
  searchIcon: {
    fontSize: "1.3em",
    transform: "translateY(-1px)"
  },
  optionIcon: {
    fontSize: "1.2em",
    marginRight: "10px",
    transform: "translateY(-1px)",
    color: "#617483"
  },
  more: {
    opacity: ".5",
    fontStyle: "normal",
    "&::before": {
      content: "' '"
    }
  },
  select: {
    lineHeight: "inherit",
    height: "100%",
    overflow: "hidden",
    "&::after": {
      content: "''",
      position: "absolute",
      left: "0",
      right: "0",
      bottom: "0",
      height: "18px",
      background: "linear-gradient( 180deg, transparent, #fff)",
      pointerEvents: "none"
    },
    "&.is-focused": {
      height: "auto",
      overflow: "visible",
      "& .Select-placeholder": {
        display: "none"
      },
      "& .Select-multi-value-wrapper": {
        position: "static"
      }
    },
    "& .Select-control": {
      display: "block",
      border: "none",
      padding: "0 80px 0 1em",
      lineHeight: "inherit",
      minHeight: "100%",
      height: "auto",
      borderRadius: "0",
      boxShadow: "none !important",
      position: "relative"
    },
    "& .Select-placeholder": {
      padding: "0",
      position: "absolute",
      left: "0",
      top: "50%",
      marginTop: "-1em",
      cursor: "text"
    },
    "& .Select-multi-value-wrapper": {
      lineHeight: "1.80em",
      padding: "1.1em 0",
      position: "absolute",
      top: "0",
      right: "80px",
      left: "1em"
    },
    "& .Select-clear-zone": {
      position: "absolute",
      top: "0",
      right: "45px",
      color: "#4b6475",
      "&:hover": {
        color: "#D0021B"
      }
    },
    "& .Select-arrow-zone": {
      position: "absolute",
      top: "-1px",
      right: "12px",
      "& .Select-arrow": {
        borderTopColor: "#4b6475"
      }
    },
    "& .Select-input": {
      height: "calc(100% - 1px)",
      margin: "0 !important",
      lineHeight: "inherit",
      "& input": {
        lineHeight: "inherit",
        padding: "0"
      }
    },
    "& .Select-value": {
      verticalAlign: "baseline",
      marginLeft: "0",
      marginRight: ".5em",
      border: "none",
      lineHeight: "18px",
      background: "#e6ebef"
    },
    "& .Select-value-icon": {
      border: "none",
      padding: "0",
      display: "inline-block",
      width: "20px",
      textAlign: "center",
      lineHeight: "20px",
      background: "#dae1e6",
      color: "#4b6475",
      height: "20px",
      "&:hover": {
        color: "#D0021B"
      }
    },
    "& .Select-value-label": {
      fontSize: "13px",
      color: "#222e4e",
      padding: "0 10px"
    },
    "& .Select-menu-outer": {
      border: "none",
      boxShadow: "0px 2px 6px rgb(161, 179, 193)",
      borderRadius: "0",
      maxHeight: "256px"
    },
    "& .Select-menu": {
      maxHeight: "256px"
    },
    "& .Select-option": {
      fontSize: ".95em",
      padding: "1em",
      lineHeight: "1.25em",
      borderTop: "1px solid rgb(243, 248, 253)",
      "&:first-child": {
        border: "none"
      },
      "&.is-focused": {
        background: "#f3f8fd"
      }
    }
  }
};

export default injectSheet(styles)(JobSelector);
