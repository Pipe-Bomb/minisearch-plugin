<h1>
    <img src="https://raw.githubusercontent.com/Pipe-Bomb/.github/refs/heads/master/assets/logos/Pipe%20Bomb%20no%20background%20w%20outline.png" width="40" />
    MiniSearch Plugin
</h1>

Uses [MiniSearch](https://github.com/lucaong/minisearch) to index Tracks, Artists and Albums to provide better search via a SearchSource.

## Installation

Clone the repo into your [Pipe Bomb server's](https://github.com/pipe-bomb/server) `plugins` directory. Then inside, run:

```bash
npm ci
npm run build
```

## Usage

Because MiniSearch's index is stored only in memory, it needs to be rebuilt each server start. You can do this by manually running the "Build index" task or you can automate it with a Workflow.

## Contributing

The MiniSearch plugin is developed by [eyezah](https://github.com/eyezahhhh), but contributions are welcome!
