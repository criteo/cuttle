// @flow

import moment from "moment";

export const displayFormat = (date: Date) =>
  moment(date).utc().format("MMM-DD HH:mm");
export const urlFormat = (date: Date) =>
  moment(date).utc().format("YYYY-MM-DDTHH") + "Z";
