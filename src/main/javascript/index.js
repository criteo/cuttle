// @flow


import React from 'react';
import { createStore, compose, applyMiddleware } from 'redux';
import { Provider } from 'react-redux';
import { render } from 'react-dom';
import ReduxThunk from 'redux-thunk';

import '../style/index.less';

import App from './layout/app';
import { initialState, reducers } from  './state';

const store = createStore(
  reducers,
  initialState,
  compose(
    applyMiddleware(ReduxThunk),
    window.devToolsExtension ? window.devToolsExtension() : _ => _
  )
)

render(
  <Provider store={store}>
    <App />
  </Provider>,
  document.getElementById('app')
)
