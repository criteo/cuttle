// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

import NodeIcon from "../../shapes/NodeIcon";
import TagBullet from "./TagBullet";
import type { Tag, NodeKind } from "../../datamodel/workflow";

import map from "lodash/map";

type Props = {
  classes: any,
  className: any,
  id: string,
  name: string,
  description: string,
  kind: NodeKind,
  tags: Tag[],
  color: string,
  active: boolean,
  verbose: boolean,
  onClick: () => void
};

const kindofNodeTitle = (kind: NodeKind) =>
  kind === "root" ? "Root node" : kind === "leaf" ? "Leaf node" : "Middle node";

const JobOverview = ({ classes, className, id, name, kind, tags, description, active }: Props) => {
  return (
    <div className={classNames(classes.job, className)}>
      
      <div className="jobBody">
        <div className="nodeKind" title={kindofNodeTitle(kind)}>
          <NodeIcon kind={kind}/>
        </div>
        <div className="name">
          {name || id}
          <div className="description">
            {description}
          </div>
        </div>
      </div>
      <ul className={classes.jobTags}>
        {map(tags, tag => (
           <li
             key={"tag" + tag.name}
             className={classes.jobTag}
           >
             <TagBullet name={tag.name} color={tag.color}/>
           </li>
         )) }
      </ul>
    </div>
  );
};

const styles = {
  job: {
    display: "flex",
    height: "inherit",
    alignItems: "center",
    justifyContent: "space-between",
    "& .nodeKind": {
      display: "inline-flex",
      alignSelf: "center"
    },
    "& .jobBody": {
      display: "inline-flex",
      "& .description": {
        color: "#7D8B99",
        fontSize: "0.8em"
      },
      "& .name": {
        marginLeft: "1.5em",
        color: "#2F3647"
      }
    }
  },
  jobTags: {
  },
  jobTag: {
    width: "1.5em",
    display: "inline-flex"
  }
};
export default injectSheet(styles)(JobOverview);
