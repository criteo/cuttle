// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";
import constant from "lodash/constant";
import Menu from "./menu";

type Props = {};

class App extends React.Component {
  props: Props;

  render() {
    return (
      <div>
        <h1>App element</h1>
        <Menu />
      </div>
    );
  }
}

let styles = {};

export const test2 = constant(3);
export default injectSheet(styles)(App);
