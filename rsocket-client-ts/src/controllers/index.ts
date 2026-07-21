/**
 * Declarative controller public surface.
 *
 * This module groups class-based controllers and their socket-scoped processor.
 */
export {
    FireAndForgetController,
    RequestChannelController,
    RequestResponseController,
    RequestStreamController
} from "@/controllers/classes.js";
export {RSocketControllerProcessor} from "@/controllers/processor.js";
export type {AnyClassController} from "@/controllers/classes.js";
export type {RSocketControllerInput} from "@/controllers/instance.js";
export type {
    ControllerArgs,
    ControllerReturn,
    RSocketControllerConstructor
} from "@/controllers/types.js";
