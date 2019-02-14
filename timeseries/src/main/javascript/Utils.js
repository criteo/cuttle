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

export class PostEventSource {
  constructor(url, body) {
    this._url = url;
    this._observers = [];
    this._body = body;
  }
  stopPolling() {
    this._poller && clearTimeout(this._poller);
  }
  startPolling() {
    if (this._poller) return;
    this._poller = 1;

    const poll = () =>
      fetch(this._url, {
        includeCredentials: true,
        method: "POST",
        body: JSON.stringify(this._body)
      })
        .then(data => data.json())
        .then(
          data => {
            this._observers.forEach(o => o({ data }));
            this._poller = setTimeout(() => poll(), 5000);
          },
          err => {
            this._poller = setTimeout(() => poll(), 15000);
          }
        );

    poll();
  }
  onmessage(observer) {
    this._observers.push(observer);
  }
}
