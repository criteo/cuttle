// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";
import Window from "../components/Window";

type Props = {
  test : string
}

const Backfill = ({props : Props}) => (<Window><div>hey</div></Window>)


const mapStateToProps = ({
  app
}) => ({
  test : "hello"
});


export default connect(mapStateToProps)(
  injectSheet({})(Backfill)
);