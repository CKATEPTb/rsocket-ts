/** Allocation-light controller lookup indexed by interaction type and route. */
import {
    FireAndForgetController,
    RequestChannelController,
    RequestResponseController,
    RequestStreamController
} from "@/controllers/classes.js";
import type {
    RSocketController,
    RSocketControllerRegistration,
    RSocketControllerRoute
} from "@/controllers/types.js";
import {normalizeRSocketRoute} from "rsocket-core-ts";

/** Internal interaction categories used as independent controller namespaces. */
export type ControllerKind = "fire-and-forget" | "request-response" | "request-stream" | "request-channel";

/** Controller together with the normalized route used by request context. */
export interface RegisteredController {
    /** Concrete controller singleton. */
    readonly controller: RSocketController;
    /** Exact ordered routing tags selecting the controller. */
    readonly route: readonly string[];
}

/** One exact route segment in an allocation-free request lookup trie. */
interface ControllerRouteNode {
    /** Controller registered at this exact route boundary. */
    registration?: RegisteredController;
    /** Lazily allocated child segments. */
    children?: Map<string, ControllerRouteNode>;
}

/** Immutable route registry built once when a server is constructed. */
export class ControllerRegistry {
    private readonly byKind: Record<ControllerKind, ControllerRouteNode> = {
        "fire-and-forget": {},
        "request-response": {},
        "request-stream": {},
        "request-channel": {}
    };

    /** Instantiates dependency-free classes and validates all registrations. */
    constructor(registrations: readonly RSocketControllerRegistration[]) {
        for (const registration of registrations) this.register(controllerInstance(registration));
    }

    /** Resolves one controller by exact interaction kind and routing tags. */
    resolve(kind: ControllerKind, route: readonly string[]): RegisteredController | undefined {
        let node = this.byKind[kind];
        for (let index = 0; index < route.length; index += 1) {
            const child = node.children?.get(route[index] as string);
            if (child === undefined) return undefined;
            node = child;
        }
        return node.registration;
    }

    /** Adds one validated singleton and rejects ambiguous duplicate routes. */
    private register(controller: RSocketController): void {
        const kind = controllerKind(controller);
        const declaredRoute = (controller as unknown as {readonly route: RSocketControllerRoute}).route;
        const route = normalizeRSocketRoute(declaredRoute);
        let node = this.byKind[kind];
        for (let index = 0; index < route.length; index += 1) {
            const tag = route[index] as string;
            const children = node.children ??= new Map<string, ControllerRouteNode>();
            let child = children.get(tag);
            if (child === undefined) {
                child = {};
                children.set(tag, child);
            }
            node = child;
        }
        if (node.registration !== undefined) {
            throw new TypeError(`Duplicate ${kind} RSocket controller route: ${route.join(" / ")}`);
        }
        node.registration = {controller, route};
    }
}

/** Returns a singleton directly or constructs one dependency-free controller. */
function controllerInstance(registration: RSocketControllerRegistration): RSocketController {
    return typeof registration === "function" ? new registration() : registration;
}

/** Determines the interaction model represented by one abstract base class. */
function controllerKind(controller: RSocketController): ControllerKind {
    if (controller instanceof FireAndForgetController) return "fire-and-forget";
    if (controller instanceof RequestResponseController) return "request-response";
    if (controller instanceof RequestStreamController) return "request-stream";
    if (controller instanceof RequestChannelController) return "request-channel";
    throw new TypeError("RSocket server received an unsupported controller registration");
}
