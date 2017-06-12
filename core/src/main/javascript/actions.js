// @flow
import type { Page } from "./ApplicationState";
import type { Project, Workflow, Statistics } from "./datamodel";

type StatusSuccess = "success";
type StatusWaiting = "pending" | "error";
type Status = StatusSuccess | StatusWaiting;
type Dispatch = (action: Action) => void;

export type Action =
  | OPEN_PAGE
  | LOAD_APP_DATA
  | UPDATE_STATISTICS
  | SELECT_JOBS;

// Action that should be dispatched by the "redux-url router"
type OPEN_PAGE = { type: "OPEN_PAGE", page: Page };
export const openPage = (page: Page): OPEN_PAGE => ({
  type: "OPEN_PAGE",
  page
});

type LOAD_ACTION<DATA_TYPE, ACTION_FLAG> =
  | {
      type: ACTION_FLAG,
      status: StatusSuccess,
      globalErrorMessage?: string,
      data: DATA_TYPE
    }
  | {
      type: ACTION_FLAG,
      status: StatusWaiting,
      globalErrorMessage?: string
    };

type LOAD_APP_DATA = LOAD_ACTION<[Project, Workflow], "LOAD_APP_DATA">;
export const loadAppData = () => (dispatch: Dispatch) => {
  dispatch(
    ({
      type: "LOAD_APP_DATA",
      status: "pending"
    }: LOAD_APP_DATA)
  );
  Promise.all([
    fetch("/api/project_definition"),
    fetch("/api/workflow_definition")
  ]).then(responses => {
    Promise.all(responses.map(r => r.json())).then(
      ([project: Project, workflow: Workflow]) =>
        dispatch(
          ({
            type: "LOAD_APP_DATA",
            status: "success",
            data: [project, workflow]
          }: LOAD_APP_DATA)
        ),
      () =>
        dispatch(
          ({
            type: "LOAD_APP_DATA",
            status: "error",
            globalErrorMessage: "Cannot parse Project definition data"
          }: LOAD_APP_DATA)
        )
    );
  });
};

type UPDATE_STATISTICS = { type: "UPDATE_STATISTICS", statistics: Statistics };
export const updateStatistics = (
  statistics: Statistics
): UPDATE_STATISTICS => ({
  type: "UPDATE_STATISTICS",
  statistics
});

type SELECT_JOBS = { type: "SELECT_JOBS", jobs: Array<string> };
export const selectJobs = (jobs: Array<string>): SELECT_JOBS => ({
  type: "SELECT_JOBS",
  jobs
});
