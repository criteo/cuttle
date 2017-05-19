export const listenEvents = (url: string, callback: (json: Any) => void) => {
  // $FlowFixMe
  let e = new EventSource(url);
  e.onmessage = msg => callback(JSON.parse(msg.data));
  return e;
};
