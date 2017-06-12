import _ from "lodash";

export const listenEvents = (
  url: string,
  callback: (json: Any) => void,
  error: ?(e: Event) => void
) => {
  // $FlowFixMe
  let minimumDelay = new Promise(resolve => setTimeout(resolve, 250));
  let e = new EventSource(url);
  let open = true;
  e.onmessage = msg =>
    minimumDelay.then(_ => open && callback(JSON.parse(msg.data)));
  error && (e.onerror = e => error(e));
  return {
    close() {
      open = false;
      e.close();
    }
  };
};
