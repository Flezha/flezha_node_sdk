import { defineUnbounded } from '@flezha/sdk';

import { EchoTasklet } from './echo_tasklet.mjs';

/**
 * The deployable unit. An operator declares the graph a message travels: the
 * entry channel it arrives on, the steps it passes through, and the channel its
 * result leaves on.
 *
 * `factoryId` is the operator's stable identity, and the name
 * `flz operator start` takes. The graph is checked as a whole when the runner
 * declares it, so one that cannot work is refused at startup.
 *
 * The entry names the text codec, so what `flz operator send --text` sends
 * arrives as a string. The sink names no codec, and a sink sends a string as its
 * UTF-8 bytes, so the output is the text itself.
 */
export default defineUnbounded({
  factoryId: 'echo',
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(EchoTasklet).sink('out');
  },
});
