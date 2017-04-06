// @flow
import type { PageId } from "./state";

type INIT = { type: "INIT" };
export const init = (): INIT => ({ type: "INIT" });

// Action that should be dispatched by the "redux-url router"
type NAVIGATE = { type: "NAVIGATE", pageId: PageId };
export const navigToPage = (pageId: PageId) => ({ type: "NAVIGATE", pageId });

export type Action = INIT | NAVIGATE;
