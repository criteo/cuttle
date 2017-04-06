// @flow

import injectSheet from "react-jss";
import classNames from "classnames";
import React from "react";

type Props = {
  iconName: string,
  classes: any,
  className: any
};

const IconComponent = ({ classes, className, iconName }: Props) => {
  return (
    <i className={classNames(classes.icon, "material-icons", className)}>
      {iconName}
    </i>
  );
};

const styles = {
  materialIcons: {
    fontFamily: "Material Icons",
    fontWeight: "normal",
    fontStyle: "normal",
    fontSize: "24px" /* Preferred icon size "*"*/,
    display: "inline-block",
    lineHeight: "1",
    textTransform: "none",
    letterSpacing: "normal",
    wordWrap: "normal",
    whiteSpace: "nowrap",
    direction: "ltr",

    /* Support for all WebKit browsers. */
    webkitFontSmoothing: "antialiased",
    /* Support for Safari and Chrome. */
    textRendering: "optimizeLegibility",
    /* Support for Firefox. */
    mozOsxFontSmoothing: "grayscale",
    /* Support for IE. */
    fontFeatureSettings: "liga"
  }
};

export default injectSheet(styles)(IconComponent);
