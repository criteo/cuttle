// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";

type Props = {};

class AppMenu extends React.Component {
  props: Props;

  render() {
    return (
      <nav>
        <ul>
          <li>test 1</li>
          <li>test 2</li>
        </ul>
        <div>User</div>
      </nav>
    );
  }
}

const styles = {};

export default injectSheet(styles)(AppMenu);
