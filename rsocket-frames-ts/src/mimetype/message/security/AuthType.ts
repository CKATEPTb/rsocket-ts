import type {ByteReader, ByteWriter} from "bebyte";
import {assertAscii} from "@/mimetype/encoding";

/**
 * Abstract base class representing an RSocket authentication type.
 *
 * Subclasses define how specific authentication payloads are serialized and deserialized.
 * Provides facilities for working with both well-known and custom authentication types.
 *
 * @template T The type of the authentication data.
 */
export abstract class AuthType<T> {
    private static readonly valuesByName = new Map<string, AuthType<any>>();
    private static readonly valuesByIdentifier = new Map<number, AuthType<any>>();
    /** Bounded fallback cache for the 128 possible unregistered wire identifiers. */
    private static readonly unknownByIdentifier: Array<AuthType<any> | undefined> = [];

    /**
     * Creates a new authentication type.
     *
     * @param authType A unique string identifier for the authentication type.
     * @param identifier An optional numeric identifier for well-known authentication types.
     * @param register Whether this codec should be available to global wire lookups.
     */
    public constructor(
        public readonly authType: string,
        public readonly identifier?: number,
        register = true
    ) {
        if (typeof authType !== "string" || authType.length === 0) {
            throw new TypeError("Authentication type must be a non-empty string")
        }
        assertAscii(authType, "Authentication type");
        if (identifier !== undefined) {
            if (!Number.isInteger(identifier) || identifier < 0 || identifier > 0x7f) {
                throw new RangeError(`Authentication type identifier must be between 0 and 127; received ${identifier}`)
            }
            if (register) AuthType.valuesByIdentifier.set(identifier, this)
        }
        if (register) AuthType.valuesByName.set(authType, this)
    }

    /**
     * @return `true` if this authentication type is a [well-known type]{@link WellKnownAuthType} (i.e., has a numeric identifier).
     */
    public get isWellKnown() {
        return this.identifier !== undefined
    }

    /**
     * Serializes the given authentication data into the provided writer.
     *
     * @param writer A [ByteWriter]{@link ByteWriter} used to serialize the data.
     * @param data The authentication data to serialize.
     */
    public abstract write(writer: ByteWriter, data: T): void

    /**
     * Deserializes authentication data from the given reader.
     *
     * @param reader A [ByteReader]{@link ByteReader} used to read the serialized data.
     * @returns The deserialized authentication data.
     */
    public abstract read(reader: ByteReader): T

    /**
     * Wraps the authentication data with this authentication type.
     *
     * @param data The authentication data.
     * @returns An object pairing the authentication type with the provided data.
     */
    public auth(data: T): { authType: AuthType<T>, data: T } {
        return {authType: this, data}
    }

    /**
     * Retrieves a registered [AuthType]{@link AuthType} by string name or numeric identifier.
     *
     * If the type is not recognized, returns a generic unknown type
     * that reads and writes raw [Uint8Array]{@link Uint8Array} data.
     *
     * @param authType A string or number representing the authentication type.
     * @returns The corresponding `AuthType` instance.
     */
    public static valueOf(authType: string | number): AuthType<any> {
        const existing = typeof authType === "string"
            ? AuthType.valuesByName.get(authType)
            : AuthType.valuesByIdentifier.get(authType)
        if (existing) return existing

        if (typeof authType === "number" && (!Number.isInteger(authType) || authType < 0 || authType > 0x7f)) {
            throw new RangeError(`Authentication type identifier must be between 0 and 127; received ${authType}`)
        }

        if (typeof authType === "number") {
            return AuthType.unknownByIdentifier[authType]
                ??= new UnknownAuthType(String(authType), authType);
        }
        return new UnknownAuthType(authType);
    }
}

/** Transient raw codec for authentication types not registered by the application. */
class UnknownAuthType extends AuthType<Uint8Array> {
    /** Creates an unregistered codec that cannot grow the global registry. */
    public constructor(authType: string, identifier?: number) {
        super(authType, identifier, false);
    }

    /** Reads all remaining custom authentication bytes without copying. */
    public read(reader: ByteReader): Uint8Array {
        return reader.viewRemaining();
    }

    /** Writes custom authentication bytes unchanged. */
    public write(writer: ByteWriter, data: Uint8Array): void {
        writer.write(data)
    }
}
