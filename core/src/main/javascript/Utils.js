export const listenEvents = (
  url: string,
  callback: (json: Any) => void,
  error: ?(e: Event) => void
) => {
  // $FlowFixMe
  let e = new EventSource(url);
  e.onmessage = msg => callback(JSON.parse(msg.data));
  error && (e.onerror = e => error(e));
  return e;
};
