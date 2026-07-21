/** Socket-scoped executor for class-based declarative controllers. */
import {controllerInstance, type RSocketControllerInput, type RSocketControllerInstanceCache} from "@/controllers/instance.js";
import {processController} from "@/controllers/process.js";
import type {AnyClassController} from "@/controllers/classes.js";
import type {ControllerArgs, ControllerReturn, RSocketControllerConnection} from "@/controllers/types.js";

/**
 * Materializes controller classes once per connection facade and dispatches
 * each call through its declared RSocket interaction model.
 */
export class RSocketControllerProcessor {
    private readonly instances: RSocketControllerInstanceCache = new WeakMap();

    /** Creates a processor over the interaction surface supplied by a client facade. */
    constructor(private readonly connection: RSocketControllerConnection) {
    }

    /** Executes one controller class or instance with its inferred argument tuple. */
    process<C extends AnyClassController>(
        definition: RSocketControllerInput<C>,
        ...args: ControllerArgs<C>
    ): ControllerReturn<C> {
        return processController(
            this.connection,
            controllerInstance(definition, this.instances),
            args
        );
    }
}
