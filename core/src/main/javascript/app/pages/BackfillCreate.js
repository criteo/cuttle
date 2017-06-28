// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";
import { compose } from "redux";
import { goBack } from "redux-url";
import { Field, reduxForm, SubmissionError } from "redux-form";
import moment from "moment";

import Window from "../components/Window";
import FancyTable from "../components/FancyTable";
import JobSelector from "../components/JobSelector";
import type { Backfill, Workflow } from "../../datamodel";

type Props = {
  workflow: Workflow,
  jobs: Array<string>,
  back: () => void,
  // Styles
  classes: any,
  // Form
  handleSubmit: any,
  error: string,
  submitting: boolean
};

const required = value => (value ? undefined : "Required");
// const jobsRequired = jobs => (jobs && jobs.length ? undefined : "Required");

const DATE_FORMAT = "YYYY-MM-DD HH";
const DATE_INVALID = `Invalid date, accepted format is ${DATE_FORMAT}`;
const validDate = value =>
  moment.utc(value).isValid() ? undefined : DATE_INVALID;

const parseDate = value => {
  const date = moment.utc(value);
  return date.isValid() ? date : value;
};
const formatDate = value => {
  const date = moment.utc(value);
  return date.isValid() ? date.format(DATE_FORMAT) : value;
};

const Label = ({ name }: any) => <dt key={name + "_"}>{name}</dt>;

const InputField = ({
  name,
  type,
  input,
  meta: { touched, error, warning }
}: any) => (
  <dd key={name}>
    <input type={type} {...input} />
    {touched &&
      ((error && <span>{error}</span>) || (warning && <span>{warning}</span>))}
  </dd>
);

const JobsField = ({ workflow, input: { value, onChange } }: any) => (
  <JobSelector workflow={workflow} selected={value} onChange={onChange} />
);

class BackfillCreate extends React.Component<any, Props, void> {
  constructor(props: Props) {
    super(props);
    (this: any).createBackfill = this.createBackfill.bind(this);
  }

  createBackfill({ jobs, name, description, start, end, priority }: Backfill) {
    if (jobs.length <= 0)
      throw new SubmissionError({ _error: "No jobs selected" });

    return fetch(
      `/api/timeseries/backfill?` +
        `name=${name}&description=${description}&` +
        ` jobs=${jobs.join(",")}&priority=${priority}&` +
        `startDate=${start.toISOString()}&endDate=${end.toISOString()}`,
      { method: "POST" }
    )
      .then((response: Response) => {
        if (!response.ok) return response.text();
        this.props.back();
        return "";
      })
      .then((text: string) => {
        if (text !== undefined && text.length > 0)
          throw new SubmissionError({ _error: text });
      })
      .catch((error: SubmissionError | any) => {
        if (error instanceof SubmissionError) throw error;
        throw new SubmissionError({ _error: error });
      });
  }

  render() {
    const { workflow, classes, handleSubmit, error, submitting } = this.props;
    return (
      <Window title="Create Backfill">
        <div className={classes.filter}>
          <Field
            name="jobs"
            workflow={workflow}
            component={JobsField}
            //TODO display error inside: validate={[jobsRequired]}
          />
        </div>
        <form onSubmit={handleSubmit(this.createBackfill)}>
          <FancyTable key="properties">
            <Label name="name" />
            <Field
              name="name"
              type="text"
              component={InputField}
              validate={[required]}
            />
            <Label name="start" />
            <Field
              name="start"
              type="text"
              component={InputField}
              format={formatDate}
              parse={parseDate}
              validate={[required, validDate]}
            />
            <Label name="end" />
            <Field
              name="end"
              type="text"
              component={InputField}
              format={formatDate}
              parse={parseDate}
              validate={[required, validDate]}
            />
            <Label name="priority" />
            <Field name="priority" type="number" component={InputField} />
            <dt name="create_" />
            <dd key="create">
              <button type="submit" disabled={submitting}>Create</button>
              {error && <strong>{error}</strong>}
            </dd>
          </FancyTable>
        </form>
      </Window>
    );
  }
}

const styles = {
  filter: {
    background: "#fff",
    height: "4em",
    lineHeight: "4em",
    boxShadow: "0px 1px 5px 0px #BECBD6"
  }
};

const mapStateToProps = ({ app: { workflow, selectedJobs } }) => ({
  workflow,
  initialValues: {
    jobs: selectedJobs,
    priority: 0,
    start: moment.utc({ hour: 0 }),
    end: moment.utc({ hour: 1 })
  }
});
const mapDispatchToProps = dispatch => ({
  back() {
    dispatch(goBack());
  }
});

export default compose(
  injectSheet(styles),
  connect(mapStateToProps, mapDispatchToProps),
  reduxForm({ form: "backfillForm" })
)(BackfillCreate);
