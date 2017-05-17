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
  | LOAD_APP_DATA
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

export const loadAppData = () =>
  (dispatch: Dispatch) => {
    dispatch({
      type: "LOAD_APP_DATA",
      status: "pending"
    });
    Promise.all([
      fetch("/api/project_definition"),
      fetch("/api/workflow_definition")
    ]).then(responses => {
      Promise.all(responses.map(r => r.json())).then(
        ([project: Project, workflow: Workflow]) =>
          dispatch({
            type: "LOAD_APP_DATA",
            status: "success",
            data: [project, workflow]
          }),
        () =>
          dispatch({
            type: "LOAD_APP_DATA",
            status: "error",
            globalErrorMessage: "Cannot parse Project definition data"
          })
      );
    });
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

export const toggleUserbar = (dispatch: Dispatch) =>
  () =>
    dispatch({
      type: "TOGGLE_USERBAR"
    });

type CLOSE_USERBAR = {
  type: "CLOSE_USERBAR"
};

export const closeUserbar = (dispatch: Dispatch) =>
  () =>
    dispatch({
      type: "CLOSE_USERBAR"
    });

type OPEN_USERBAR = {
  type: "OPEN_USERBAR"
};

export const openUserbar = (dispatch: Dispatch) =>
  () =>
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
