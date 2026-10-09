import {
	DataClient,
	Logger,
	SavedAlbum,
	SavedArtist,
	SavedTrack,
} from "@pipe-bomb/plugin-sdk";
import { MiniSearchSearchSource, RebuildListener } from "./search-source.js";
import { Action, AnyQueueItem, QueueItem } from "./types/queue.type.js";

const MAX_BATCH = 50;
const FLUSH_INTERVAL_MS = 1000;

function yieldToEventLoop(): Promise<void> {
	return new Promise((resolve) => setImmediate(resolve));
}

export class UpdateQueue implements RebuildListener {
	private readonly queue: AnyQueueItem[] = [];
	private readonly queuedTracks = new Map<string, Action>();
	private readonly queuedArtists = new Map<string, Action>();
	private readonly queuedAlbums = new Map<string, Action>();
	private isHandlingQueue = false;
	private paused = false;
	private lastFlushAt = 0;

	constructor(
		private readonly dataClient: DataClient,
		private readonly searchSource: MiniSearchSearchSource,
		private readonly logger: Logger,
	) {
		searchSource.setRebuildListener(this);

		// subscribe to track updates
		dataClient.addListener("track-added", (track) =>
			this.addTrack(track, "update"),
		);
		dataClient.addListener("track-attributes-updated", (track) =>
			this.addTrack(track, "update"),
		);
		dataClient.addListener("track-artists-updated", (track) =>
			this.addTrack(track, "update"),
		);
		dataClient.addListener("track-removed", (track) =>
			this.addTrack(track, "delete"),
		);

		// subscribe to artist updates
		dataClient.addListener("artist-added", (artist) =>
			this.addArtist(artist, "update"),
		);
		dataClient.addListener("artist-attributes-updated", (artist) =>
			this.addArtist(artist, "update"),
		);
		dataClient.addListener("artist-removed", (artist) =>
			this.addArtist(artist, "delete"),
		);

		// subscribe to album updates
		dataClient.addListener("album-added", (album) =>
			this.addAlbum(album, "update"),
		);
		dataClient.addListener("album-attributes-updated", (album) =>
			this.addAlbum(album, "update"),
		);
		dataClient.addListener("album-artists-updated", (album) =>
			this.addAlbum(album, "update"),
		);
		dataClient.addListener("album-removed", (album) =>
			this.addAlbum(album, "delete"),
		);
	}

	private addTrack(track: SavedTrack, action: Action) {
		const queuedAction = this.queuedTracks.get(track.uuid);
		if (queuedAction) {
			if (queuedAction == action) {
				return;
			}
			const index = this.queue.findIndex(
				(e) => e.type == "track" && e.item.uuid == track.uuid,
			);
			if (index >= 0) {
				this.queue.splice(index, 1);
			}
		}

		this.queuedTracks.set(track.uuid, action);
		this.queue.push({
			type: "track",
			item: track,
            action
		});
		this.handleQueue();
	}

	private addArtist(artist: SavedArtist, action: Action) {
		const queuedAction = this.queuedArtists.get(artist.uuid);
		if (queuedAction) {
			if (queuedAction == action) {
				return;
			}
			const index = this.queue.findIndex(
				(e) => e.type == "artist" && e.item.uuid == artist.uuid,
			);
			if (index >= 0) {
				this.queue.splice(index, 1);
			}
		}

		this.queuedArtists.set(artist.uuid, action);
		this.queue.push({
			type: "artist",
			item: artist,
            action
		});
		this.handleQueue();
	}

	private addAlbum(album: SavedAlbum, action: Action) {
		const queuedAction = this.queuedAlbums.get(album.uuid);
		if (queuedAction) {
			if (queuedAction == action) {
				return;
			}
			const index = this.queue.findIndex(
				(e) => e.type == "album" && e.item.uuid == album.uuid,
			);
			if (index >= 0) {
				this.queue.splice(index, 1);
			}
		}

		this.queuedAlbums.set(album.uuid, action);
		this.queue.push({
			type: "album",
			item: album,
            action
		});
		this.handleQueue();
	}

	onRebuildStart(): void {
		this.paused = true;
		this.clear();
	}

	onRebuildEnd(): void {
		this.paused = false;
		void this.handleQueue();
	}

	private clear(): void {
		this.queue.length = 0;
		this.queuedTracks.clear();
		this.queuedArtists.clear();
		this.queuedAlbums.clear();
	}

	async handleQueue(): Promise<void> {
		if (this.paused || this.isHandlingQueue) {
			return;
		}

		this.isHandlingQueue = true;
		try {
			while (!this.paused && this.queue.length > 0) {
				await this.processBatch();
				await yieldToEventLoop();
				if (
					!this.paused &&
					(this.queue.length === 0 ||
						Date.now() - this.lastFlushAt >= FLUSH_INTERVAL_MS)
				) {
					await this.searchSource.flushSorted();
					this.lastFlushAt = Date.now();
				}
			}
		} catch (e) {
			this.logger.error("Failed to update search database:", e);
		} finally {
			this.isHandlingQueue = false;
			if (!this.paused && this.queue.length > 0) {
				setImmediate(() => void this.handleQueue());
			}
		}
	}

	private async processBatch(): Promise<void> {
		const firstEntry = this.queue.shift();
		if (!firstEntry) {
			return;
		}

		const itemType = firstEntry.type;

		const updateChunk: AnyQueueItem[] = [];
		const deleteChunk: AnyQueueItem[] = [];
		if (firstEntry.action === "update") {
			updateChunk.push(firstEntry);
		} else {
			deleteChunk.push(firstEntry);
		}

		const removeIndexes: number[] = [];
		for (
			let i = 0;
			i < this.queue.length &&
			updateChunk.length < MAX_BATCH &&
			deleteChunk.length < MAX_BATCH;
			i++
		) {
			const entry = this.queue[i]!;
			if (entry.type === itemType) {
				if (entry.action === "update") {
					updateChunk.push(entry);
				} else {
					deleteChunk.push(entry);
				}
				removeIndexes.push(i);
			}
		}
		if (removeIndexes.length > 0) {
			const removed = new Set(removeIndexes);
			let write = 0;
			for (let read = 0; read < this.queue.length; read++) {
				if (!removed.has(read)) {
					this.queue[write++] = this.queue[read]!;
				}
			}
			this.queue.length = write;
		}

		for (const entry of updateChunk) {
			this.clearQueued(entry);
		}
		for (const entry of deleteChunk) {
			this.clearQueued(entry);
		}

		switch (itemType) {
			case "track": {
				const ids = updateChunk
					.filter((e): e is QueueItem<"track"> => e.type === "track")
					.map(({ item }) => ({
						pluginId: item.pluginId,
						libraryId: item.libraryId,
						trackId: item.trackId,
					}));
				if (ids.length > 0) {
					const tracks = await this.dataClient.getTracks(ids, {
						relations: {
							attributes: true,
							artists: { attributes: true },
						},
					});
					this.searchSource.updateTracks(tracks);
				}
				this.searchSource.deleteTracks(
					deleteChunk
						.filter((e): e is QueueItem<"track"> => e.type === "track")
						.map(({ item }) => item.uuid),
				);
				break;
			}
			case "artist": {
				const uuids = updateChunk
					.filter((e): e is QueueItem<"artist"> => e.type === "artist")
					.map(({ item }) => item.uuid);
				if (uuids.length > 0) {
					const artists = await this.dataClient.getArtists(uuids, {
						relations: { attributes: true },
					});
					this.searchSource.updateArtists(artists);
				}
				this.searchSource.deleteArtists(
					deleteChunk
						.filter((e): e is QueueItem<"artist"> => e.type === "artist")
						.map(({ item }) => item.uuid),
				);
				break;
			}
			case "album": {
				const uuids = updateChunk
					.filter((e): e is QueueItem<"album"> => e.type === "album")
					.map(({ item }) => item.uuid);
				if (uuids.length > 0) {
					const albums = await this.dataClient.getAlbums(uuids, {
						relations: {
							attributes: true,
							artists: { attributes: true },
						},
					});
					this.searchSource.updateAlbums(albums);
				}
				this.searchSource.deleteAlbums(
					deleteChunk
						.filter((e): e is QueueItem<"album"> => e.type === "album")
						.map(({ item }) => item.uuid),
				);
				break;
			}
			default: {
				const _exhaustive: never = itemType;
				throw new Error(`Unhandled queue item type: ${_exhaustive}`);
			}
		}
	}

	private clearQueued(entry: AnyQueueItem): void {
		switch (entry.type) {
			case "track":
				this.queuedTracks.delete(entry.item.uuid);
				break;
			case "artist":
				this.queuedArtists.delete(entry.item.uuid);
				break;
			case "album":
				this.queuedAlbums.delete(entry.item.uuid);
				break;
		}
	}
}
