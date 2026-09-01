import type PipeBomb from "@pipe-bomb/plugin-sdk";
import { PipeBombSearchSource } from "./search-source.js";

export default class Plugin implements PipeBomb.Plugin {
	enable(apiContext: PipeBomb.PluginApiContext): void {
		const logger = apiContext.getLogger();
		const dataClient = apiContext.getDataClient();
		const source = new PipeBombSearchSource(dataClient, logger);

		apiContext.registerLanguageDirectory("language");
		apiContext.registerSearchSource(source);

		apiContext.registerTask({
			id: "build-index",
			resumable: false,
			run: async (ctx) => source.buildIndex((p) => ctx.update(p)),
		});
	}

	disable(): void {}
}
