/**
 * Class-controller materialization helpers used by `RSocket.process(...)`.
 */
import type {AnyClassController} from "@/controllers/classes.js";
import type {RSocketControllerConstructor} from "@/controllers/types.js";

/**
 * Controller instance or zero-argument controller class accepted by `process(...)`.
 * Class declarations are instantiated once per `RSocket`; pass an explicit
 * instance when application-controlled mutable controller state is required.
 */
export type RSocketControllerInput<C extends AnyClassController> = C | RSocketControllerConstructor<C>;

/**
 * Per-socket cache for declarative controller class instances.
 *
 * Class declarations are stateless definitions, like singleton Spring
 * controllers. Reusing their instance also reuses encoded route metadata.
 */
export type RSocketControllerInstanceCache = WeakMap<
    RSocketControllerConstructor<AnyClassController>,
    AnyClassController
>;

/**
 * Creates a controller instance when a request method receives a controller class.
 */
export function controllerInstance<C extends AnyClassController>(
    controllerDefinition: RSocketControllerInput<C>,
    cache?: RSocketControllerInstanceCache
): C {
    if (typeof controllerDefinition === "function") {
        const cached = cache?.get(controllerDefinition);
        if (cached !== undefined) return cached as C;
        const instance = new controllerDefinition();
        cache?.set(controllerDefinition, instance);
        return instance;
    }
    return controllerDefinition;
}
