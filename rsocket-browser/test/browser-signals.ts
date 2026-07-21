/** Installs controllable browser availability signals for reconnect tests. */
export function installBrowserSignals(initialOnline: boolean) {
  const target = new EventTarget();
  const originalAdd = (globalThis as any).addEventListener;
  const originalRemove = (globalThis as any).removeEventListener;
  const originalNavigatorDescriptor = Object.getOwnPropertyDescriptor(globalThis, "navigator");
  const navigatorLike = globalThis.navigator ?? {};
  const originalOnlineDescriptor = Object.getOwnPropertyDescriptor(navigatorLike, "onLine");

  (globalThis as any).addEventListener = target.addEventListener.bind(target);
  (globalThis as any).removeEventListener = target.removeEventListener.bind(target);
  if (globalThis.navigator === undefined) {
    Object.defineProperty(globalThis, "navigator", {
      configurable: true,
      value: navigatorLike
    });
  }

  const setOnline = (online: boolean): void => {
    Object.defineProperty(navigatorLike, "onLine", {
      configurable: true,
      value: online
    });
  };
  setOnline(initialOnline);

  return {
    /** Dispatches one browser wake signal. */
    dispatch(type: string): void {
      target.dispatchEvent(new Event(type));
    },
    /** Updates the fake `navigator.onLine` value. */
    setOnline,
    /** Restores every global changed by this fixture. */
    restore(): void {
      if (originalAdd === undefined) delete (globalThis as any).addEventListener;
      else (globalThis as any).addEventListener = originalAdd;
      if (originalRemove === undefined) delete (globalThis as any).removeEventListener;
      else (globalThis as any).removeEventListener = originalRemove;
      if (originalOnlineDescriptor === undefined) delete (navigatorLike as any).onLine;
      else Object.defineProperty(navigatorLike, "onLine", originalOnlineDescriptor);
      if (originalNavigatorDescriptor === undefined) delete (globalThis as any).navigator;
      else Object.defineProperty(globalThis, "navigator", originalNavigatorDescriptor);
    }
  };
}
