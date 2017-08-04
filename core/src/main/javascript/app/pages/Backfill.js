// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";
import Window from "../components/Window";

type Props = {
  backfillId : string
}

class Backfill extends React.Component {
  props : Props

  constructor(props: Props) {
    super(props);
  }

  render() {
    return (<Window>
      Heya
    </Window>)
  }
}

export default injectSheet({})(Backfill)