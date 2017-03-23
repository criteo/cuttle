// @flow


import React from 'react';
import { connect } from 'react-redux';
import injectSheet from 'react-jss';
import classNames from 'classnames';
import moment from 'moment';
import _ from 'lodash';

import Menu from './menu';

type Props = {
  classes: {[key: string]: string},
  loaded: boolean,
  loadData: () => void,
  error: ?string
};

class App extends React.Component {
  props: Props;

  renderError(error: string) {
    return (
      <strong className={this.props.classes.error}>
        {error}
      </strong>
    )
  }

  render() {
    <div>
      <Menu />
    </div>

  }
}

let styles = {
  loading: {
    width: '100vw',
    height: '100vh',
    paddingTop: 100,
    background: '#374054'
  },
  error: {
    display: 'block',
    textAlign: 'center',
    marginTop: 30,
    color: '#E53935',
    fontWeight: 400
  }
};

export default injectSheet(styles)(App);
