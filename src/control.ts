/**
 * Administering the namespace this runner's connection proved, from ordinary
 * application code.
 *
 * No tasklet has to be running and the application holds no credential of
 * its own: the controller judges every call as the identity the connection
 * already established, so nothing an unattended process holds expires. An
 * user grants a namespace's applications the authority once and revokes it
 * the same way, and a revocation takes effect on the next call:
 *
 * ```sh
 * flz role bind runner@team-orders app team-orders
 * ```
 *
 * Every method here is one line over a single native entry point, because the
 * verb travels as data. Adding a verb costs a method and no native work.
 */
import type { Native } from './native.js';

/** What a relay start takes beside its factory, which is what `flz relay start` takes. */
export interface StartOptions {
  /**
   * The relay's own id. Without one, every start runs a new relay
   * under a generated id, so a start retried after a lost answer starts a
   * second one; with one, a second start leaves the running relay untouched.
   */
  id?: string;
  /** The values the factory is seeded with, keyed by tasklet. */
  init_values?: unknown;
  /** The start options, as `flz relay start` takes them. */
  opts?: unknown;
}

/** What a plugin enable takes beside its name and provider, which is what `flz plugin enable` takes. */
export interface PluginOptions {
  /** The provider's options. */
  opts?: Record<string, unknown>;
  /** Run one copy in the whole cluster. */
  single?: boolean;
  /** A relay to bind in the same call, with `hooks`. */
  relay_id?: string;
  /** The hooks the relay binds, as names or `[name, opts]` pairs. */
  hooks?: unknown[];
}

/** What a plugin bind takes beside the plugin and the relay. */
export interface BindOptions {
  /** The hooks the relay binds, as names or `[name, opts]` pairs. */
  hooks?: unknown[];
  /** The channel the plugin feeds or reads. */
  channel?: string;
}

/** What a start did: `already_running` only for an id given while it runs. */
export type StartOutcome = 'started' | 'already_running';

/** A start's answer: the id the relay runs under, given or generated. */
export interface StartResult {
  id: string;
  outcome: StartOutcome;
}

export class Control {
  constructor(private readonly native: Native) {}

  /** Start a relay of `factoryId`. */
  async startRelay(factoryId: string, options: StartOptions = {}): Promise<StartResult> {
    const { id, outcome } = (await this.call('Relay.start', {
      ...options,
      factory_id: factoryId,
    })) as StartResult;
    return { id, outcome };
  }

  /** Stop a relay, or the whole partition group it names. */
  async stopRelay(relayId: string): Promise<unknown> {
    return this.call('Relay.stop', relayId);
  }

  /**
   * Enable the plugin `name` in this namespace, of the provider `provider`
   * (for example `relay_websocket`).
   */
  async enablePlugin(
    name: string,
    provider: string,
    options: PluginOptions = {},
  ): Promise<unknown> {
    return this.call('PluginManager.enable', name, { opts: {}, ...options, provider });
  }

  /** Bind the relay `relayId` to the enabled plugin `name`. */
  async bindPlugin(name: string, relayId: string, options: BindOptions = {}): Promise<unknown> {
    return this.call('PluginManager.bind', name, { ...options, relay_id: relayId });
  }

  /** Unbind the relay `relayId` from the plugin `name`, which stays enabled. */
  async unbindPlugin(name: string, relayId: string): Promise<unknown> {
    return this.call('PluginManager.unbind', name, { relay_id: relayId });
  }

  /** Disable the plugin `name`, stopping it wherever it runs. */
  async disablePlugin(name: string): Promise<unknown> {
    return this.call('PluginManager.disable', name, {});
  }

  /**
   * The relays in this namespace, one row each: its id, health, runner and node, and a partition
   * group's width and members.
   */
  async listRelays(): Promise<unknown> {
    return this.call('Relay.list');
  }

  /**
   * The plugins enabled in this namespace, one row each: name, provider, single, bound relays and
   * where it runs.
   */
  async listPlugins(): Promise<unknown> {
    return this.call('PluginManager.enabled');
  }

  /**
   * A relay's live state, and the change sequence it was read at. `cell` is
   * `<tasklet>[/<cell_key>]`: a tasklet's Cells, or one of them; absent, every
   * Cell of the relay.
   */
  async cellState(relayId: string, cell?: string): Promise<unknown> {
    return this.call('Relay.cell_state', relayId, cell);
  }

  /**
   * Any published function, by name, for a verb this surface has not yet grown
   * a method for. What succeeds follows from the role the namespace is bound
   * to, never from a list kept here. `args` are the call's arguments other
   * than the namespace, which the controller fills from the connection.
   */
  async call(verb: string, ...args: unknown[]): Promise<unknown> {
    return JSON.parse(await this.native.adminCall(verb, JSON.stringify(args)));
  }
}
