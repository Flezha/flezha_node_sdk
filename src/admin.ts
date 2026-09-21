/**
 * Administering the namespace this runner's connection proved, from ordinary
 * application code.
 *
 * No tasklet has to be running and the application holds no credential of
 * its own: the controller judges every call as the identity the connection
 * already established, so nothing an unattended process holds expires. An
 * operator grants a namespace's applications the authority once and revokes it
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

/** What an operative start takes beside its factory, which is what `flz operative start` takes. */
export interface StartOptions {
  /**
   * The operative's own id. Without one, every start runs a new operative
   * under a generated id, so a start retried after a lost answer starts a
   * second one; with one, a second start leaves the running operative untouched.
   */
  id?: string;
  /** `'stream'` (the default) or `'workflow'`. */
  kind?: string;
  /** The values the factory is seeded with, keyed by tasklet. */
  init_values?: unknown;
  /** The start options, as `flz operative start` takes them. */
  opts?: unknown;
}

/** What a start did: `already_running` only for an id given while it runs. */
export type StartOutcome = 'started' | 'already_running';

/** A start's answer: the id the operative runs under, given or generated. */
export interface StartResult {
  id: string;
  outcome: StartOutcome;
}

export class Admin {
  constructor(private readonly native: Native) {}

  /** Start an operative of `factoryId`. */
  async startOperative(factoryId: string, options: StartOptions = {}): Promise<StartResult> {
    const { id, outcome } = (await this.call('Operative.start', {
      ...options,
      factory_id: factoryId,
    })) as StartResult;
    return { id, outcome };
  }

  /** Stop an operative, or the whole partition group it names. */
  async stopOperative(operativeId: string): Promise<unknown> {
    return this.call('Operative.stop', operativeId);
  }

  /**
   * Inject a message into an operative's input stream and block for the
   * delivery verdict. `channel` may be empty for none.
   */
  async send(operativeId: string, message: string, channel = ''): Promise<unknown> {
    return this.call('Operative.send', operativeId, message, channel);
  }

  /** Enable a plugin, built from the fields `flz plugin enable` takes. */
  async enablePlugin(pluginId: string, fields: Record<string, unknown>): Promise<unknown> {
    return this.call('PluginManager.enable', pluginId, fields);
  }

  /** Disable a plugin, or every partitioned instance of it. */
  async disablePlugin(pluginId: string): Promise<unknown> {
    return this.call('PluginManager.disable', pluginId);
  }

  /** The operative ids running in this namespace. */
  async listOperatives(): Promise<unknown> {
    return this.call('Operative.list_operatives');
  }

  /** The plugins enabled on the controller node answering this call. */
  async listPlugins(): Promise<unknown> {
    return this.call('PluginManager.list');
  }

  /** One cell's live state, and the change sequence it was read at. */
  async cellState(operativeId: string, cellKey: string): Promise<unknown> {
    return this.call('Operative.cell_state', operativeId, cellKey);
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
