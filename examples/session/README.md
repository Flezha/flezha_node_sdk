# Flezha Node local-mode example

A two-tasklet word count run in this process with no controller. `Split` and
`Tally` are ordinary tasklets: the same definition deploys against a controller
unchanged. Local mode only supplies the output, so nothing here is durable.

```bash
npm install
npm run local
```

See [Local mode](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/running_and_testing.md#local-mode).
