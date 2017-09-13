export const listenEvents = (
  url: string,
  callback: (json: any) => void,
  error: ?(e: Event) => void
) => {
  const minimumDelay = new Promise(resolve => setTimeout(resolve, 250));
  const keepOpenInterval = setInterval(keepOpen, 10000);
  let eventSource = undefined;
  let open = true;
  let isAlive = false;

  function keepOpen(firstOpen) {
    if (!isAlive && open) {
      !firstOpen && error && error("connection lost");
      eventSource && eventSource.close();
      eventSource = new EventSource(url);
      eventSource.onmessage = msg =>
        minimumDelay.then(() => {
          isAlive = true;
          open && callback(JSON.parse(msg.data));
        });

      error &&
        (eventSource.onerror = e => {
          isAlive = true;
          error(e);
        });
    }
    isAlive = false;
  }

  keepOpen(true);

  return {
    close() {
      open = false;
      clearInterval(keepOpenInterval);
      eventSource.close();
    }
  };
};
