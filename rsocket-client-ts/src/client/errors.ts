/** Client-specific failures raised by requester interaction policies. */
import {RSocketError} from "rsocket-core-ts";

/** Raised when lease enforcement prevents the client from opening a request stream. */
export class RSocketLeaseError extends RSocketError {
    /** Creates a client lease error. */
    constructor(message: string) {
        super(message);
        this.name = "RSocketLeaseError";
    }
}
