// @flow

import React from "react";
import { connect } from "react-redux";
import injectSheet from "react-jss";
import { compose } from "redux";
import { goBack } from "redux-url";
import { Field, reduxForm, SubmissionError } from "redux-form";
import moment from "moment";
import ArrowIcon from "react-icons/lib/fa/long-arrow-right";

import Window from "../components/Window";
import FancyTable from "../components/FancyTable";
import JobSelector from "../components/JobSelector";
import type { Backfill, ExecutionLog, Workflow } from "../../datamodel";
import MaskedInput from "react-maskedinput";
import StreamView from "../components/StreamView";
import TextWithDashedLine from "../components/TextWithDashedLine";
import Error from "../components/Error";
import ReactTooltip from "react-tooltip";
import InfoIcon from "react-icons/lib/md/info";

type Props = {
  workflow: Workflow,
  jobs: Array<string>,
  back: () => void,
  // Styles
  classes: any,
  // Form
  handleSubmit: any,
  error: string,
  valid: boolean,
  submitting: boolean,
  env: {
    name: ?string,
    critical: boolean
  }
};

const REQUIRED_MESSAGE = "Required";
const DATE_FORMAT = "YYYY-MM-DD HH:mm";
const DATE_INVALID = `Invalid date, accepted format is ${DATE_FORMAT}`;
const INTEGER_INVALID = "Should be an integer";

const required = value => (value != undefined ? undefined : REQUIRED_MESSAGE);
const jobsRequired = jobs =>
  jobs && jobs.length ? undefined : REQUIRED_MESSAGE;
const validDate = value =>
  moment.utc(value, DATE_FORMAT, true).isValid() ? undefined : DATE_INVALID;
const validInteger = value =>
  Number.isSafeInteger(parseFloat(value)) ? undefined : INTEGER_INVALID;

const parseDate = value => {
  const date = moment.utc(value, DATE_FORMAT, true);
  return date.isValid() ? date : value;
};
const formatDate = value => {
  const date = moment.utc(value, DATE_FORMAT, true);
  return date.isValid() ? date.format(DATE_FORMAT) : value;
};

const Label = ({ name, style = null }: any) => (
  <dt key={`_${name}`} style={style}>
    {name}
  </dt>
);

const InputField = ({
  name,
  type,
  input,
  placeholder,
  meta: { touched, error, warning },
  alwaysDisplayError
}: any) => (
  <span>
    <input
      className="input-field"
      type={type}
      {...input}
      placeholder={placeholder}
    />
    {(touched || alwaysDisplayError) &&
      ((error && <span className="input-error">{error}</span>) ||
        (warning && <span className="input-warning">{warning}</span>))}
  </span>
);

const TextAreaField = ({
  name,
  input,
  placeholder,
  meta: { touched, error, warning }
}) => (
  <span>
    <textarea
      className="input-field markdown-area"
      placeholder={placeholder}
      {...input}
    />
    {touched &&
      ((error && <span className="input-error">{error}</span>) ||
        (warning && <span className="input-warning">{warning}</span>))}
  </span>
);

const validateConfirmation = environment => value => {
  const errorMessage =
    "You are going to run a backfill, please type in the name of the environment to confirm.";
  return environment && environment === value ? undefined : errorMessage;
};

const JobsField = ({ workflow, input: { value, onChange } }: any) => (
  <JobSelector workflow={workflow} selected={value} onChange={onChange} />
);

const DateWithMask = ({
  name,
  type,
  input,
  placeholder,
  meta: { touched, error, warning }
}: any) => (
  <span>
    <MaskedInput
      mask="1111-11-11 11:11"
      className="input-field"
      type={type}
      {...input}
      placeholder={placeholder}
    />
    {touched &&
      ((error && <span className="input-error">{error}</span>) ||
        (warning && <span className="input-warning">{warning}</span>))}
  </span>
);

class BackfillCreate extends React.Component<Props> {
  constructor(props: Props) {
    super(props);
    (this: any).createBackfill = this.createBackfill.bind(this);
  }

  createBackfill({ jobs, name, description, start, end, priority }: Backfill) {
    if (jobs.length <= 0)
      throw new SubmissionError({ _error: "No jobs selected" });

    return fetch(`/api/timeseries/backfill`, {
      method: "POST",
      credentials: "include",
      body: JSON.stringify({
        name: name,
        description: description || "",
        jobs: jobs.join(","),
        priority: priority,
        startDate: start.toISOString(),
        endDate: end.toISOString()
      })
    })
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
    const {
      workflow,
      classes,
      handleSubmit,
      error,
      valid,
      submitting,
      env
    } = this.props;

    ReactTooltip.rebuild();

    return (
      <Window title="Create Backfill">
        <form
          onSubmit={handleSubmit(this.createBackfill)}
          className={classes.createBackfillForm}
        >
          <FancyTable key="properties">
            <Label name="Name" />
            <dd key="name">
              <Field
                name="name"
                type="text"
                component={InputField}
                validate={[required]}
                props={{ placeholder: "To identify the backfill" }}
              />
            </dd>
            <Label name="Jobs to backfill" />
            <dd name="jobs" style={{ overflow: "visible" }}>
              <div className={classes.filter}>
                <Field
                  name="jobs"
                  workflow={workflow}
                  component={JobsField}
                  props={{ placeholder: "Select the list of jobs to backfill" }}
                  validate={[jobsRequired]}
                />
              </div>
            </dd>
            <Label name="Period" />
            <dd name="period" className={classes.dateRangeSelector}>
              <Field
                name="start"
                type="text"
                component={DateWithMask}
                format={formatDate}
                parse={parseDate}
                validate={[required, validDate]}
              />
              <ArrowIcon />
              <Field
                name="end"
                type="text"
                component={DateWithMask}
                format={formatDate}
                parse={parseDate}
                validate={[required, validDate]}
              />
            </dd>
            <Label name="Priority" />
            <dd name="priority">
              <Field
                name="priority"
                type="number"
                component={InputField}
                validate={[required, validInteger]}
                props={{ alwaysDisplayError: true }}
              />
              <InfoIcon style={styles.icon}
                        data-for="backfill-priority-tooltip"
                        data-tip="The lower the number, the higher the priority of the backfilled execution(s)."/>
              <ReactTooltip
                  id="backfill-priority-tooltip"
                  effect="float"/>
            </dd>
            <dt key={"description_"} className="double-height">
              {"Description"}
            </dt>
            <dd name="description" className="double-height">
              <Field
                name="description"
                component={TextAreaField}
                props={{
                  placeholder:
                    "Explain why you need to backfill. Markdown is supported."
                }}
              />
            </dd>
            <div
              style={{ display: "flex", width: "100%" }}
              className={env.critical ? "confirm confirm-critical" : "confirm"}
            >
              <Label name="Confirm" style={{ color: "white" }} />
              <dd name="confirm">
                <Field
                  name="confirm"
                  component={InputField}
                  validate={[validateConfirmation(env.name)]}
                  props={{ alwaysDisplayError: true }}
                />
              </dd>
            </div>
            <dt name="create_" />
            <dd key="create">
              <button
                type="submit"
                className={["validate-button " + (valid ? "" : "error")]}
                disabled={valid == false || submitting}
              >
                Start to backfill
              </button>
            </dd>
          </FancyTable>
        </form>
        {error && <Error message={error} />}
      </Window>
    );
  }
}

const styles = {
  filter: {
    background: "#fff",
    height: "4em",
    lineHeight: "4em"
  },
  dateRangeSelector: {
    "& > span:last-child": {
      marginLeft: "10px"
    },
    "& > svg": {
      marginLeft: "10px"
    }
  },
  createBackfillForm: {
    "& .input-field": {
      border: "1px solid #D8D8D9",
      padding: "5px",
      borderRadius: "3px"
    },
    "& .input-field.markdown-area": {
      verticalAlign: "middle",
      width: "95%",
      height: "70px",
      resize: "none"
    },
    "& .confirm": {
      backgroundColor: "#26A69A",
      color: "white"
    },
    "& .confirm.confirm-critical": {
      backgroundColor: "#FF5722"
    },
    "& .validate-button": {
      padding: "10px",
      borderRadius: "3px",
      backgroundColor: "#283249",
      borderColor: "transparent",
      color: "white",
      fontWeight: "bold"
    },
    "& .validate-button.error": {
      backgroundColor: "#6A6A6A",
      cursor: "not-allowed"
    },
    "& dt": {
      lineHeight: "4em"
    },
    "& dd": {
      lineHeight: "4em"
    },
    "& dt.double-height": {
      lineHeight: "8em"
    },
    "& dd.double-height": {
      lineHeight: "8em"
    },
    "& .input-error": {
      marginLeft: "5px"
    },
    "& .input-warning": {
      marginLeft: "5px"
    },
    "& input::-webkit-input-placeholder, textarea::-webkit-input-placeholder": {
      color: "#aaa"
    },
    "& input:-moz-placeholder, textarea:-moz-placeholder": {
      /* Mozilla Firefox 4 to 18 */
      color: "#aaa",
      opacity: 1
    },
    "& input::-moz-placeholder, textarea::-moz-placeholder": {
      /* Mozilla Firefox 19+ */
      color: "#aaa",
      opacity: 1
    },
    "& input:-ms-input-placeholder, textarea:-ms-input-placeholder": {
      /* Internet Explorer 10-11 */
      color: "#aaa"
    },
    "& input::-ms-input-placeholder, textarea::-ms-input-placeholder": {
      /* Microsoft Edge */
      color: "#aaa"
    }
  },
  icon: {
    fontSize: "22px"
  }
};

const mapStateToProps = ({ app: { workflow, selectedJobs, project } }) => ({
  workflow,
  initialValues: {
    jobs: selectedJobs,
    priority: 0,
    start: moment.utc({ hour: 0 }),
    end: moment.utc({ hour: 1 })
  },
  env: project && project.env
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
