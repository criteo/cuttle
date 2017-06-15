export const listenEvents = (
  url: string,
  callback: (json: any) => void,
  error: ?(e: Event) => void
) => {
  let minimumDelay = new Promise(resolve => setTimeout(resolve, 250));
  let e = new EventSource(url);
  let open = true;
  e.onmessage = msg =>
    minimumDelay.then(() => open && callback(JSON.parse(msg.data)));
  error && (e.onerror = e => error(e));
  return {
    close() {
      open = false;
      e.close();
    }
  };
};
