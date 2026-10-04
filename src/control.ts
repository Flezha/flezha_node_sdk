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

/** What an operator start takes beside its factory, which is what `flz operator start` takes. */
export interface StartOptions {
  /**
   * The operator's own id. Without one, every start runs a new operator
   * under a generated id, so a start retried after a lost answer starts a
   * second one; with one, a second start leaves the running operator untouched.
   */
  id?: string;
  /** The values the factory is seeded with, keyed by tasklet. */
  init_values?: unknown;
  /** The start options, as `flz operator start` takes them. */
  opts?: unknown;
}

/** What a plugin enable takes beside its name and provider, which is what `flz plugin enable` takes. */
export interface PluginOptions {
  /** The provider's options. */
  opts?: Record<string, unknown>;
  /** Run one copy in the whole cluster. */
  single?: boolean;
  /** An operator to bind in the same call, with `hooks`. */
  operator_id?: string;
  /** The hooks the operator binds, as names or `[name, opts]` pairs. */
  hooks?: unknown[];
}

/** What a plugin bind takes beside the plugin and the operator. */
export interface BindOptions {
  /** The hooks the operator binds, as names or `[name, opts]` pairs. */
  hooks?: unknown[];
  /** The channel the plugin feeds or reads. */
  channel?: string;
}

/** What a start did: `already_running` only for an id given while it runs. */
export type StartOutcome = 'started' | 'already_running';

/** A start's answer: the id the operator runs under, given or generated. */
export interface StartResult {
  id: string;
  outcome: StartOutcome;
}

export class Control {
  constructor(private readonly native: Native) {}

  /** Start an operator of `factoryId`. */
  async startOperator(factoryId: string, options: StartOptions = {}): Promise<StartResult> {
    const { id, outcome } = (await this.call('Operator.start', {
      ...options,
      factory_id: factoryId,
    })) as StartResult;
    return { id, outcome };
  }

  /** Stop an operator, or the whole partition group it names. */
  async stopOperator(operatorId: string): Promise<unknown> {
    return this.call('Operator.stop', operatorId);
  }

  /**
   * Enable the plugin `name` in this namespace, of the provider `provider`
   * (for example `operator_websocket`).
   */
  async enablePlugin(
    name: string,
    provider: string,
    options: PluginOptions = {},
  ): Promise<unknown> {
    return this.call('PluginManager.enable', name, { opts: {}, ...options, provider });
  }

  /** Bind the operator `operatorId` to the enabled plugin `name`. */
  async bindPlugin(name: string, operatorId: string, options: BindOptions = {}): Promise<unknown> {
    return this.call('PluginManager.bind', name, { ...options, operator_id: operatorId });
  }

  /** Unbind the operator `operatorId` from the plugin `name`, which stays enabled. */
  async unbindPlugin(name: string, operatorId: string): Promise<unknown> {
    return this.call('PluginManager.unbind', name, { operator_id: operatorId });
  }

  /** Disable the plugin `name`, stopping it wherever it runs. */
  async disablePlugin(name: string): Promise<unknown> {
    return this.call('PluginManager.disable', name, {});
  }

  /** The operator ids running in this namespace. */
  async listOperators(): Promise<unknown> {
    return this.call('Operator.list_operators');
  }

  /** The plugins enabled in this namespace: name, provider, single, bound operators. */
  async listPlugins(): Promise<unknown> {
    return this.call('PluginManager.list');
  }

  /** One cell's live state, and the change sequence it was read at. */
  async cellState(operatorId: string, cellKey: string): Promise<unknown> {
    return this.call('Operator.cell_state', operatorId, cellKey);
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
