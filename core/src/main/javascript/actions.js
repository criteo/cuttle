// @flow
import type { PageId } from "./state";
import type Project from "./datamodel/project";
import type { Workflow } from "./datamodel/workflow";

type StatusSuccess = "success";
type StatusWaiting = "pending" | "error";
type Status = StatusSuccess | StatusWaiting;
type Dispatch = (action: Action) => void;

export type Action =
  | INIT
    | NAVIGATE
    | LOAD_PROJECT_DATA
    | LOAD_WORKFLOW_DATA
    | SELECT_JOB
    | DESELECT_JOB
    | TOGGLE_USERBAR
    | OPEN_USERBAR
    | CLOSE_USERBAR
    | CHANGE_JOBSEARCH_INPUT
    | SELECT_FILTERTAG
    | DESELECT_FILTERTAG
    | TOGGLE_FILTERTAG;

// Actions
type INIT = { type: "INIT" };
export const init = (): INIT => ({ type: "INIT" });

// Action that should be dispatched by the "redux-url router"
type NAVIGATE = { type: "NAVIGATE", pageId: PageId };
export const navigToPage = (pageId: PageId): NAVIGATE => ({
  type: "NAVIGATE",
  pageId
});

type LOAD_ACTION<DATA_TYPE, ACTION_FLAG> = {
  type: ACTION_FLAG,
  status: StatusSuccess,
  globalErrorMessage?: string,
  data: DATA_TYPE
} | {
  type: ACTION_FLAG,
  status: StatusWaiting,
  globalErrorMessage?: string
};

type LOAD_PROJECT_DATA = LOAD_ACTION<Project, "LOAD_PROJECT_DATA">;

export const loadProjectData = (dispatch: Dispatch) => () => {
  dispatch({
    type: "LOAD_PROJECT_DATA",
    status: "pending"
  });
  fetch("/api/project_definition").then(
    response => {
      response.json().then(
        (project_definition: Project) =>
          dispatch({
            type: "LOAD_PROJECT_DATA",
            status: "success",
            data: project_definition
          }),
        () =>
          dispatch({
            type: "LOAD_PROJECT_DATA",
            status: "error",
            globalErrorMessage: "Cannot parse Project definition data"
          })
      );
    },
    () =>
      dispatch({
        type: "LOAD_PROJECT_DATA",
        status: "error",
        globalErrorMessage: "Cannot load Project definition data"
      })
  );
};

type LOAD_WORKFLOW_DATA = LOAD_ACTION<Workflow, "LOAD_WORKFLOW_DATA">;

export const loadWorkflowData = (dispatch: Dispatch) => () => {
  dispatch({
    type: "LOAD_WORKFLOW_DATA",
    status: "pending"
  });
  fetch("/api/workflow_definition").then(
    response => {
      response.json().then(
        (workflow_definition: Workflow) =>
          dispatch({
            type: "LOAD_WORKFLOW_DATA",
            status: "success",
            data: workflow_definition
          }),
        () =>
          dispatch({
            type: "LOAD_WORKFLOW_DATA",
            status: "error",
            globalErrorMessage: "Cannot parse Workflow definition data"
          })
      );
    },
    () =>
      dispatch({
        type: "LOAD_WORKFLOW_DATA",
        status: "error",
        globalErrorMessage: "Cannot load Workflow definition data"
      })
  );
};

// Jobs selection

type SELECT_JOB = {
  type: "SELECT_JOB",
  jobId: string
};

export const selectJob = (dispatch: Dispatch) =>
  (id: string) =>
    dispatch({
      type: "SELECT_JOB",
      jobId: id
    });

type DESELECT_JOB = {
  type: "DESELECT_JOB",
  jobId: string
};

export const deselectJob = (dispatch: Dispatch) =>
  (id: string) =>
    dispatch({
      type: "DESELECT_JOB",
      jobId: id
    });

// Userbar

type TOGGLE_USERBAR = {
  type: "TOGGLE_USERBAR"
};

export const toggleUserbar = (dispatch: Dispatch) => () =>
  dispatch({
    type: "TOGGLE_USERBAR"
  });

type CLOSE_USERBAR = {
  type: "CLOSE_USERBAR"
};

export const closeUserbar = (dispatch: Dispatch) => () =>
  dispatch({
    type: "CLOSE_USERBAR"
  });

type OPEN_USERBAR = {
  type: "OPEN_USERBAR"
};

export const openUserbar = (dispatch: Dispatch) => () =>
  dispatch({
    type: "OPEN_USERBAR"
  });

// Job filtering

type CHANGE_JOBSEARCH_INPUT = {
  type: "CHANGE_JOBSEARCH_INPUT",
  inputText: string
};

export const changeJobSearchInput = (dispatch: Dispatch) =>
  (text: string) =>
    dispatch({
      type: "CHANGE_JOBSEARCH_INPUT",
      inputText: text
    });

type SELECT_FILTERTAG = {
  type: "SELECT_FILTERTAG",
  tagName: string
};

export const selectFilterTag = (dispatch: Dispatch) =>
  (tagName: string) =>
    dispatch({
      type: "SELECT_FILTERTAG",
      tagName
    });

type DESELECT_FILTERTAG = {
  type: "DESELECT_FILTERTAG",
  tagName: string
};

export const deselectFilterTag = (dispatch: Dispatch) =>
  (tagName: string) =>
    dispatch({
      type: "SELECT_FILTERTAG",
      tagName
    });

type TOGGLE_FILTERTAG = {
  type: "TOGGLE_FILTERTAG",
  tagName: string
};

export const toggleFilterTag = (dispatch: Dispatch) =>
  (tagName: string) =>
    dispatch({
      type: "TOGGLE_FILTERTAG",
      tagName
    });
