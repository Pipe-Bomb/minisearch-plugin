import type PipeBomb from "@pipe-bomb/plugin-sdk";
import { MiniSearchSearchSource } from "./search-source.js";
import { UpdateQueue } from "./update-queue.js";

export default class Plugin implements PipeBomb.Plugin {
	enable(apiContext: PipeBomb.PluginApiContext): void {
		const logger = apiContext.getLogger();
		const dataClient = apiContext.getDataClient();
		const source = new MiniSearchSearchSource(dataClient, logger);

		apiContext.registerLanguageDirectory("language");
		apiContext.registerSearchSource(source);

		const updateQueue = new UpdateQueue(dataClient, source, logger);

		apiContext.registerTask({
			id: "build-index",
			resumable: false,
			run: async (ctx) => source.buildIndex((p) => ctx.update(p)),
		});
	}

	disable(): void {}
}
